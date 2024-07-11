package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
)

type table struct {
	sst *file.SSTable
	lm  *levelManager
	fid uint64
}

// openTable 打开 sst 文件，builder 不为空时将数据写到 sst 文件
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	size := int(0)
	if builder != nil {
		size = builder.done().size
	} else {
		size = int(lm.opt.SSTableMaxSz)
	}
	sst := file.OpenSSTable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    size,
	})
	t := &table{
		sst: sst,
		lm:  lm,
		fid: utils.FID(tableName),
	}

	if builder != nil {
		if err := builder.flush(sst); err != nil {
			utils.Err(err)
			return nil
		}
	}

	if err := t.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return t
}

// Search 在 sst 文件中查找 key
func (t *table) Search(key []byte, maxVs *uint64) (*utils.Entry, error) {
	idx := t.sst.Indexs()
	// 先用 Bloom 看数据是否存在
	bloomFilter := utils.Filter(idx.BloomFilter)
	if t.sst.HasBloomFilter() && !bloomFilter.MayContainKey(key) {
		return nil, utils.ErrKeyNotFound
	}

	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFound
	}

	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFound
}

func (t *table) indexKey() uint64 {
	return t.fid
}

func (t *table) getEntry(key, block []byte, idx int) (*utils.Entry, error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &utils.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}

// block 加载 sst 文件索引为 idx 的文件（可能使用缓存）
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("idx=%d", idx))
	if idx >= len(t.sst.Indexs().Offsets) {
		return nil, errors.New("block out of index")
	}

	var b *block
	key := t.blockCacheKey(idx)
	// 先去缓存中查，有的话直接返回
	blk, ok := t.lm.cache.blocks.Get(key)
	if ok && blk != nil {
		b = blk.(*block)
		return b, nil
	}

	var ko pb.BlockOffset
	utils.CondPanic(!t.offsets(&ko, idx), fmt.Errorf("block t.offset id=%d", idx))
	b = &block{
		offset: int(ko.GetOffset()),
	}

	var err error
	// 读取该 block 块的所有数据
	if b.data, err = t.read(b.offset, int(ko.GetLen())); err != nil {
		return nil, errors.Wrapf(err, "faild to read from sst: %d at offset: %d, len: %d",
			t.sst.FID(), b.offset, ko.GetLen())
	}

	// 读出校验和及其长度
	readPos := len(b.data) - 4
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))
	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := readPos
	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])

	b.data = b.data[:readPos+4]
	if err = b.verifyChecksum(); err != nil {
		return nil, err
	}

	// 将当前 block 块信息添加到缓存中
	t.lm.cache.blocks.Set(key, b)
	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

// blockCacheKey 用在缓存时的 key
// key 格式 sst.id sst.block_idx
func (t *table) blockCacheKey(idx int) []byte {
	buf := make([]byte, 8)
	binary.BigEndian.PutUint32(buf[:4], uint32(t.fid))
	binary.BigEndian.PutUint32(buf[4:], uint32(idx))
	return buf
}

type tableIterator struct {
	opt      *utils.Options
	item     utils.Item
	t        *table // 当前指向的 sst 文件
	bi       *blockIterator
	blockPos int   // 当前指向的 block 索引
	err      error // 记录错误
}

func (t *table) NewIterator(opt *utils.Options) utils.Iterator {
	return &tableIterator{
		opt: opt,
		t:   t,
		bi:  &blockIterator{},
	}
}

func (it *tableIterator) Next() {
}

func (it *tableIterator) Valid() bool {
	return it == nil
}

func (it *tableIterator) Rewind() {
}

func (it *tableIterator) Item() utils.Item {
	return it.item
}

func (it *tableIterator) Close() error {
	return nil
}

// Seek 利用二分查找在当前 sst 文件找 key
// TODO(qingw1230): 查找逻辑需要再思考一下
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	// idx block.minKey > key 的索引
	idx := sort.Search(len(it.t.sst.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx),
			fmt.Errorf("tableIterator.Seek idx < 0 || idx > len(index.GetOffsets())"))
		return utils.CompareKeys(ko.GetKey(), key) > 0
	})

	if idx == 0 {
		it.seekHelper(0, key)
		return
	}
	it.seekHelper(idx-1, key)
	if it.err == io.EOF {
		if idx == len(it.t.sst.Indexs().Offsets) {
			return
		}
		it.seekHelper(idx, key)
	}
}

// seekHelper 加载索引为 blockIdx 的 block，并在里面查找 key
func (it *tableIterator) seekHelper(blockIdx int, key []byte) {
	it.blockPos = blockIdx
	block, err := it.t.block(blockIdx)
	if err != nil {
		it.err = err
		return
	}

	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	it.bi.seek(key)
	it.err = it.bi.Error()
	it.item = it.bi.Item()
}

// offsets 将 ko 设置为 block_offsets[i]
func (t *table) offsets(ko *pb.BlockOffset, i int) bool {
	index := t.sst.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	*ko = *index.GetOffsets()[i]
	return true
}
