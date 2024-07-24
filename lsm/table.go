package lsm

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
)

type table struct {
	sst *file.SSTable // sst 文件
	lm  *levelManager // 所属 levelManager
	fid uint64        // sst 文件标识
	ref int32         // 引用计数，用于文件垃圾收集
}

// openTable 打开 sst 文件，加载 sst 文件索引部分，builder 不为空时将数据写到 sst 文件
func openTable(lm *levelManager, tableName string, builder *tableBuilder) *table {
	sstSize := int(lm.opt.SSTableMaxSz)
	if builder != nil {
		sstSize = int(builder.done().size)
	}

	var (
		t   *table
		err error
	)
	fid := utils.FID(tableName)

	// 将 builder flush 到磁盘
	if builder != nil {
		if t, err = builder.flush(lm, tableName); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		// 打开一个已经存在的 sst 文件
		t = &table{
			lm:  lm,
			fid: fid,
		}
		t.sst = file.OpenSSTable(&file.Options{
			FileName: tableName,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(sstSize),
		})
	}

	t.IncrRef()
	// 根据 sst 文件初始化结构体
	if err = t.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	// 默认是降序
	it := t.NewIterator(&utils.Options{})
	defer it.Close()
	it.Rewind()
	maxKey := it.Item().Entry().Key
	t.sst.SetMaxKey(maxKey)
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
		return nil, errors.Wrapf(err, "faild to read from sst: %d at offset: %d, len: %d", t.sst.FID(), b.offset, ko.GetLen())
	}

	// 读出校验和及其长度
	readPos := len(b.data) - 4
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))
	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]

	b.data = b.data[:readPos]
	if err = b.verifyChecksum(); err != nil {
		return nil, err
	}

	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIndexStart := readPos - (numEntries * 4)
	entriesIndexEnd := readPos
	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIndexStart:entriesIndexEnd])
	// 需要根据该值读取 data 数据
	b.entriesIndexStart = entriesIndexStart

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

// tableIterator sst 文件迭代器
type tableIterator struct {
	opt      *utils.Options
	item     utils.Item     // 存储数据
	t        *table         // 当前指向的 sst 文件
	bi       *blockIterator // sst 内 block 迭代器
	blockPos int            // 当前指向的 block 索引
	err      error          // 记录错误
}

// NewIterator 创建用于遍历 sst 文件的迭代器
func (t *table) NewIterator(opt *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		opt: opt,
		t:   t,
		bi:  &blockIterator{},
	}
}

// Next 指向 sst 文件下一个 key，一个 block 遍历完就遍历下一个
func (it *tableIterator) Next() {
	if it.blockPos >= len(it.t.sst.Indexs().GetOffsets()) {
		it.err = io.EOF
		return
	}

	it.err = nil
	// 此时遍历到一个新的 sst 文件
	if len(it.bi.data) == 0 {
		block, err := it.t.block(it.blockPos)
		if err != nil {
			it.err = err
			return
		}
		it.bi.tableID = it.t.fid
		it.bi.blockID = it.blockPos
		it.bi.setBlock(block)
		it.bi.seekToFirst()
		it.item = it.bi.Item()
		it.err = it.bi.Error()
		return
	}

	it.bi.Next()
	// 当前 block 遍历完了，遍历下一个
	if !it.bi.Valid() {
		it.blockPos++
		it.bi.data = nil
		it.Next()
		return
	}
	it.item = it.bi.Item()
}

func (it *tableIterator) Valid() bool {
	return it.err != io.EOF
}

// Rewind 定位到遍历需要的第一个 key
func (it *tableIterator) Rewind() {
	if it.opt.IsAsc {
		it.seekToFirst()
	} else {
		it.seekToLast()
	}
}

func (it *tableIterator) Item() utils.Item {
	return it.item
}

func (it *tableIterator) Close() error {
	it.bi.Close()
	return it.t.DecrRef()
}

// seekToFirst 定位到当前 sst 文件第一个 block 的第一个 key
func (it *tableIterator) seekToFirst() {
	numBlocks := len(it.t.sst.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}

	it.blockPos = 0
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	// 将 bi 指向所属块的第一个 key
	it.bi.seekToFirst()
	it.item = it.bi.Item()
	it.err = it.bi.Error()
}

// seekToLast 定位到当前 sst 文件最后一个 block 的最后一个 key
func (it *tableIterator) seekToLast() {
	numBlocks := len(it.t.sst.Indexs().Offsets)
	if numBlocks == 0 {
		it.err = io.EOF
		return
	}

	it.blockPos = numBlocks - 1
	block, err := it.t.block(it.blockPos)
	if err != nil {
		it.err = err
		return
	}
	it.bi.tableID = it.t.fid
	it.bi.blockID = it.blockPos
	it.bi.setBlock(block)
	// 将 bi 指向所属块的最后一个 key
	it.bi.seekToLast()
	it.item = it.bi.Item()
	it.err = it.bi.Error()
}

// Seek 利用二分查找在当前 sst 文件找 key
// TODO(qingw1230): 查找逻辑需要再思考一下
func (it *tableIterator) Seek(key []byte) {
	var ko pb.BlockOffset
	// idx block.minKey > key 的索引
	idx := sort.Search(len(it.t.sst.Indexs().GetOffsets()), func(idx int) bool {
		utils.CondPanic(!it.t.offsets(&ko, idx), fmt.Errorf("tableIterator.Seek idx < 0 || idx >= len(index.GetOffsets())"))
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

// Size 返回 sst 文件大小
func (t *table) Size() int64 {
	return int64(t.sst.Size())
}

// GetCreatedAt 返回 sst 文件创建时间
func (t *table) GetCreatedAt() *time.Time {
	return t.sst.GetCreatedAt()
}

// Delete 删除 sst 文件
func (t *table) Delete() error {
	return t.sst.Detele()
}

// StaleDataSize 返回 该 sst 文件中过期数据大小
func (t *table) StaleDataSize() uint32 {
	return t.sst.Indexs().StaleDataSize
}

// DecrReg 减少该 sst 文件的引用计数，无人引用时删除该文件
func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		for i := 0; i < len(t.sst.Indexs().GetOffsets()); i++ {
			t.lm.cache.blocks.Del(t.blockCacheKey(i))
		}
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// IncrRef 增加该 sst 文件的引用计数
func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

// decrRefs 减少一组 sst 文件的引用计数
func decrRefs(tables []*table) error {
	for _, t := range tables {
		if err := t.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}
