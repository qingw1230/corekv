package lsm

import (
	"fmt"
	"io"
	"os"
	"sort"
	"sync/atomic"
	"time"

	"github.com/pkg/errors"
	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
)

type table struct {
	lm  *levelManager
	sst *file.SSTable
	fid uint64
	ref int32
}

func openTable(lm *levelManager, name string, builder *tableBuilder) (t *table) {
	var err error
	fid := utils.FID(name)

	if builder != nil {
		if t, err = builder.flush(lm, name); err != nil {
			utils.Err(err)
			return nil
		}
	} else {
		t = &table{
			lm:  lm,
			fid: fid,
		}
		t.sst = file.OpenSSTable(&file.Options{
			FileName: name,
			Dir:      lm.opt.WorkDir,
			Flag:     os.O_CREATE | os.O_RDWR,
			MaxSz:    int(lm.opt.SSTableMaxSz),
		})
	}

	t.IncrRef()
	if err := t.sst.Init(); err != nil {
		utils.Err(err)
		return nil
	}
	return
}

// Search 从 sst 中查找 key
func (t *table) Search(key []byte, maxVs *uint64) (e *utils.Entry, err error) {
	t.IncrRef()
	defer t.DecrRef()

	iter := t.NewIterator(&utils.Options{})
	defer iter.Close()

	iter.Seek(key)
	if !iter.Valid() {
		return nil, utils.ErrKeyNotFount
	}

	if utils.SameKey(key, iter.Item().Entry().Key) {
		if version := utils.ParseTs(iter.Item().Entry().Key); *maxVs < version {
			*maxVs = version
			return iter.Item().Entry(), nil
		}
	}
	return nil, utils.ErrKeyNotFount
}

// block 加载 sst 对应的 block
func (t *table) block(idx int) (*block, error) {
	utils.CondPanic(idx < 0, fmt.Errorf("table.block idx=%d", idx))
	if idx >= len(t.sst.Indexs().GetOffsets()) {
		return nil, errors.New("block out of index")
	}

	// TODO(qingw1230): 从缓存中获取

	var bo pb.BlockOffset
	utils.CondPanic(!t.offsets(&bo, idx), fmt.Errorf("block t.offset id=%d", idx))
	b := &block{
		offset: int(bo.GetOffset()),
	}

	var err error
	if b.data, err = t.read(b.offset, int(bo.GetLen())); err != nil {
		return nil, errors.Wrapf(err, "faild to read from sstable: %d at offset: %d, len: %d", t.sst.FID(), b.offset, bo.GetLen())
	}

	// 读取校验和长度
	readPos := len(b.data) - 4
	b.chkLen = int(utils.BytesToU32(b.data[readPos : readPos+4]))
	if b.chkLen > len(b.data) {
		return nil, errors.New("invalid checksum length")
	}
	// 读取校验和
	readPos -= b.chkLen
	b.checksum = b.data[readPos : readPos+b.chkLen]
	// 验证校验和
	b.data = b.data[:readPos]
	if err = b.verifyChecksum(); err != nil {
		return nil, err
	}

	// 读取 entry 个数
	readPos -= 4
	numEntries := int(utils.BytesToU32(b.data[readPos : readPos+4]))
	entriesIdexStart := readPos - (numEntries * 4)
	entriesIdexEnd := readPos

	b.entryOffsets = utils.BytesToU32Slice(b.data[entriesIdexStart:entriesIdexEnd])
	b.entryIndexStart = entriesIdexStart

	return b, nil
}

func (t *table) read(off, sz int) ([]byte, error) {
	return t.sst.Bytes(off, sz)
}

func (t *table) Size() int64 {
	return int64(t.sst.Size())
}

// StaleDataSize 返回该 sst 文件中过期数据的大小
func (t *table) StaleDataSize() uint32 {
	return t.sst.Indexs().StaleDataSize
}

func (t *table) GetCreatedAt() *time.Time {
	return t.sst.GetCreatedAt()
}

func (t *table) Delete() error {
	return t.sst.Delete()
}

func (t *table) IncrRef() {
	atomic.AddInt32(&t.ref, 1)
}

func (t *table) DecrRef() error {
	newRef := atomic.AddInt32(&t.ref, -1)
	if newRef == 0 {
		if err := t.Delete(); err != nil {
			return err
		}
	}
	return nil
}

// decrRefs 减少一组 sst 文件的计数，当计数为 0 时删除文件
func decrRefs(tables []*table) error {
	for _, t := range tables {
		if err := t.DecrRef(); err != nil {
			return err
		}
	}
	return nil
}

// offsets 让 bo 指向第 i 个 block
func (t *table) offsets(bo *pb.BlockOffset, i int) bool {
	index := t.sst.Indexs()
	if i < 0 || i > len(index.GetOffsets()) {
		return false
	}
	if i == len(index.GetOffsets()) {
		return true
	}
	*bo = *index.GetOffsets()[i]
	return true
}

type tableIterator struct {
	t        *table
	opt      *utils.Options
	bi       *blockIterator
	blockPos int
	item     utils.Item
	err      error
}

func (t *table) NewIterator(opt *utils.Options) utils.Iterator {
	t.IncrRef()
	return &tableIterator{
		t:        t,
		opt:      opt,
		bi:       &blockIterator{},
		blockPos: 0,
	}
}

func (ti *tableIterator) Next() {
	ti.err = nil

	if ti.blockPos >= len(ti.t.sst.Indexs().GetOffsets()) {
		ti.err = io.EOF
		return
	}

	if len(ti.bi.data) == 0 {
		b, err := ti.t.block(ti.blockPos)
		if err != nil {
			ti.err = err
			return
		}
		ti.bi.tableID = ti.t.fid
		ti.bi.blockID = ti.blockPos
		ti.bi.setBlock(b)
		ti.bi.seekToFirst()
		ti.err = ti.bi.Error()
		return
	}

	ti.bi.Next()
	// 当前 block 已经遍历完了，去下一 block
	if !ti.bi.Valid() {
		ti.blockPos++
		ti.bi.data = nil
		ti.Next()
		return
	}
	ti.item = ti.bi.item
}

func (ti *tableIterator) seekToFirst() {
	numBlocks := len(ti.t.sst.Indexs().GetOffsets())
	if numBlocks == 0 {
		ti.err = io.EOF
		return
	}

	ti.blockPos = 0
	b, err := ti.t.block(ti.blockPos)
	if err != nil {
		ti.err = err
		return
	}
	ti.bi.tableID = ti.t.fid
	ti.bi.blockID = ti.blockPos
	ti.bi.setBlock(b)
	ti.bi.seekToFirst()
	ti.err = ti.bi.Error()
	ti.item = ti.bi.Item()
}

func (ti *tableIterator) seekToLast() {
	numBlocks := len(ti.t.sst.Indexs().GetOffsets())
	if numBlocks == 0 {
		ti.err = io.EOF
		return
	}

	ti.blockPos = numBlocks - 1
	b, err := ti.t.block(ti.blockPos)
	if err != nil {
		ti.err = err
		return
	}
	ti.bi.tableID = ti.t.fid
	ti.bi.blockID = ti.blockPos
	ti.bi.setBlock(b)
	ti.bi.seekToLast()
	ti.err = ti.bi.Error()
	ti.item = ti.bi.Item()
}

func (ti *tableIterator) Valid() bool {
	return ti.err != io.EOF
}

// Seek 在 sst 中利用二分查找指定 key
func (ti *tableIterator) Seek(key []byte) {
	var bo pb.BlockOffset
	n := len(ti.t.sst.Indexs().GetOffsets())
	idx := sort.Search(n, func(idx int) bool {
		utils.CondPanic(!ti.t.offsets(&bo, idx), fmt.Errorf("tableutils.Seek idx < 0 || idx > len(index.GetOffsets()"))
		if idx == n {
			return true
		}
		return utils.CompareKeys(bo.GetKey(), key) > 0
	})
	if idx == 0 {
		ti.seekHelper(0, key)
		return
	}
	ti.seekHelper(idx-1, key)
}

// seekHelper 在 sst 第 blockIdx 个 block 块中查找 key
func (ti *tableIterator) seekHelper(blockIdx int, key []byte) {
	ti.blockPos = blockIdx
	b, err := ti.t.block(blockIdx)
	if err != nil {
		ti.err = err
		return
	}
	ti.bi.tableID = ti.t.fid
	ti.bi.blockID = ti.blockPos
	ti.bi.setBlock(b)
	ti.bi.seek(key)
	ti.err = ti.bi.Error()
	ti.item = ti.bi.Item()
}

func (ti *tableIterator) Item() utils.Item {
	return ti.item
}

func (ti *tableIterator) Rewind() {
	if ti.opt.IsAsc {
		ti.seekToFirst()
	} else {
		ti.seekToLast()
	}
}

func (ti *tableIterator) Close() error {
	ti.bi.Close()
	return ti.t.DecrRef()
}
