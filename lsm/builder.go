package lsm

import (
	"errors"
	"fmt"
	"io"
	"os"
	"sort"
	"unsafe"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/pb"
	"github.com/qingw1230/corekv/utils"
	"google.golang.org/protobuf/proto"
)

type tableBuilder struct {
	opt           *Options
	curBlock      *block   // 当前使用的 block
	blockList     []*block // 所有 block 的列表
	keyCount      uint32   // 所有 block key 总数
	keyHashes     []uint32 // 所有 key 的 hash 值列表
	maxVersion    uint64   // 当前的最大版本号
	sstSize       int64
	staleDataSize int   // 过期数据大小
	estimateSz    int64 // sst 文件预估大小
}

// block sst 文件的一个 block 块
// block 格式   | kv_data | offsets | offset_len | checksum | checksum_len |
// kv_data 格式 | header | diffkey | expires_at | value |
// header 格式  | overlap | diff |
type block struct {
	offset          int      // 当前 block 在 sst 文件的偏移
	data            []byte   // 存储 block 所有数据
	baseKey         []byte   // 当前 block 的公共 key，第一个插入的数据作为 baseKey
	entryOffsets    []uint32 // 各个 key 的偏移
	entryIndexStart int      // entryOffsets 起始位置
	end             int      // 当前结尾位置
	estimateSz      int64    // block 预估大小
	checksum        []byte   // 当前 block 的校验和
	chkLen          int      // 校验和长度
}

// buildData builder.done() 生成的数据结构体
// sst 格式          | block1 | block2 | ... | sst_index | index_len | checksum | checksum_len |
// sst_index 格式    | block_offsets | bloom_filter | max_version  | key_count |
// block_offset 格式 | key | offset | len |
type buildData struct {
	blockList []*block // 所有 block 列表
	index     []byte   // pb 序列化后的索引
	checksum  []byte   // 索引的校验和
	size      int      // 该 builder 总长度
}

// header KV 数据的头部
type header struct {
	overlap uint16 // 与 baseKey 重叠部分长度
	diff    uint16 // 与 baseKey 不同部分长度
}

func (h header) encode() []byte {
	var b [4]byte
	*(*header)(unsafe.Pointer(&b[0])) = h
	return b[:]
}
func (h *header) decode(buf []byte) {
	copy((*[headerSize]byte)(unsafe.Pointer(h))[:], buf[:headerSize])
}

const headerSize = uint16(unsafe.Sizeof(header{}))

func newTAbleBuilderWithSSTSize(opt *Options, sz int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: sz,
	}
}

func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

// flush 将 builder 数据写入 sst 文件
func (tb *tableBuilder) flush(lm *levelManager, name string) (*table, error) {
	bd := tb.done()
	t := &table{
		lm:  lm,
		fid: utils.FID(name),
	}
	t.sst = file.OpenSSTable(&file.Options{
		FileName: name,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size),
	})
	buf := make([]byte, bd.size)
	// TODO(qingw1230): 有多次拷贝，需要优化
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush writen != len(buf)"))
	dst, err := t.sst.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

func (t *tableBuilder) empty() bool {
	return len(t.keyHashes) == 0
}

// finish 结束当前 builder，将 sst 所有数据写入切片
func (t *tableBuilder) finish() []byte {
	bd := t.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written == len(buf), nil)
	return buf
}

func (t *tableBuilder) Close() {
}

func (tb *tableBuilder) AddKey(e *utils.Entry) {
	tb.add(e, false)
}

// add 将一个 entry 添加到 block 中
func (tb *tableBuilder) add(e *utils.Entry, _ bool) {
	key := e.Key
	// 检查是否需要分配一个新的 block
	if tb.tryFinishBlock(e) {
		tb.finishBlock()
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	val := utils.ValueStruct{
		Meta:      e.Meta,
		Value:     e.Value,
		ExpiresAt: e.ExpiresAt,
	}
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey, key...)
		diffKey = key
	} else {
		diffKey = tb.keyDiff(key)
	}

	tb.curBlock.entryOffsets = append(tb.curBlock.entryOffsets, uint32(tb.curBlock.end))

	h := header{
		overlap: uint16(len(key) - len(diffKey)),
		diff:    uint16(len(diffKey)),
	}
	tb.append(h.encode())
	tb.append(diffKey)
	dst := tb.allocate(int(val.EncodedSize()))
	val.EncodeValue(dst)
}

// keyDiff 返回 newKey 与 t.curBlock.baseKey 不同的部分
func (t *tableBuilder) keyDiff(newKey []byte) []byte {
	var i int
	for i = 0; i < len(newKey) && i < len(t.curBlock.baseKey); i++ {
		if newKey[i] != t.curBlock.baseKey[i] {
			break
		}
	}
	return newKey[i:]
}

// Copy 将 bd 数据拷贝到 dst
func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, b := range bd.blockList {
		written += copy(dst[written:], b.data[:b.end])
	}

	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))
	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// done 结束当前 builder，生成 bloom 和 block_offset 等索引数据
func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	bd := buildData{
		blockList: tb.blockList,
	}

	if tb.opt.BloomFalsePositive > 0 {
	}

	index, dataSize := tb.buildIndex(nil)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4 // index_len & checksum_len
	return bd
}

// buildIndex 使用 tb 数据构建它的索引
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)
	if len(bloom) > 0 {
	}
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.KeyCount = tb.keyCount

	var dataSize uint32
	for _, b := range tb.blockList {
		dataSize += uint32(b.end)
	}
	data, err := proto.Marshal(tableIndex)
	utils.Panic(err)
	return data, dataSize
}

// writeBlockOffsets 将 t.blockList 信息写入 []*pb.BlockOffset
func (t *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, b := range t.blockList {
		offset := t.writeBlockOffset(b, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(b.end)
	}
	return offsets
}

// writeBlockOffset 将 block 信息写入 *pb.BlockOffset
func (t *tableBuilder) writeBlockOffset(b *block, startOffset uint32) *pb.BlockOffset {
	offset := &pb.BlockOffset{}
	offset.Key = b.baseKey
	offset.Offset = startOffset
	offset.Len = uint32(b.end)
	return offset
}

func (t *tableBuilder) ReachedCapacity() bool {
	return t.estimateSz > t.sstSize
}

// tryFinishBlock 检查当前 block 是否已满
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) == 0 {
		return false
	}

	// entryOffsetsSize block 块索引所需大小
	entryOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 + // offsets
		4 + // offset_len
		8 + // checksum
		4) // checksum_len

	tb.curBlock.estimateSz = int64(tb.curBlock.end) +
		int64(6 /* header size for entry */) + int64(len(e.Key)) + int64(e.EncodedSize()) +
		entryOffsetsSize

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

// finishBlock 结束当前 block
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}

	tb.curBlock.entryIndexStart = tb.curBlock.end

	// 追加所有 key 的偏移及其长度
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	// 追加校验和（KVs & offsets & offset_len）及其长度
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))

	tb.estimateSz += tb.curBlock.estimateSz
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.curBlock.checksum = checksum
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil
}

// append 向 t.curBlock.data 追加数据
func (t *tableBuilder) append(data []byte) {
	dst := t.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

// allocate 在 tb.curBlock.data 申请 need 字节空间，会修改 curBlock.end
func (tb *tableBuilder) allocate(need int) []byte {
	b := tb.curBlock
	if len(b.data[b.end:]) < need {
		sz := 2 * len(b.data)
		if b.end+need > sz {
			sz = b.end + need
		}

		buf := make([]byte, sz)
		copy(buf, b.data)
		b.data = buf
	}
	b.end += need
	return b.data[b.end-need : b.end]
}

func (t *tableBuilder) calculateChecksum(data []byte) []byte {
	checksum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checksum)
}

func (b *block) verifyChecksum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	block        *block   // 当前指向的 block
	data         []byte   // block 的 KV 数据部分
	entryOffsets []uint32 // block 内各 key 的偏移
	idx          int      // key 在 entryOffsets 的下标
	err          error
	baseKey      []byte
	key          []byte // 指向的 key
	val          []byte

	tableID uint64 // 当前 block 所属 sst 文件
	blockID int    // 在所属 sst 文件 blockList 的下标

	prevOverlap uint16 // 上一个 key 重叠部分长度

	item utils.Item // 保存数据
}

// seek 在 block 中查找指定 key
func (bi *blockIterator) seek(key []byte) {
	bi.err = nil
	startIdx := 0

	// 利用二分查找找 f(idx) 为 true 的最小索引
	// 即找第一个大于等于指定 key 的索引
	foundEntryIdx := sort.Search(len(bi.entryOffsets), func(idx int) bool {
		if idx < startIdx {
			return false
		}
		bi.setIdx(idx)
		return utils.CompareKeys(bi.key, key) >= 0
	})
	bi.setIdx(foundEntryIdx)
}

// setBlock 将 bi 指向指定 block
func (bi *blockIterator) setBlock(b *block) {
	bi.block = b
	bi.data = b.data[:b.entryIndexStart]
	bi.entryOffsets = b.entryOffsets
	bi.idx = 0
	bi.err = nil
	bi.baseKey = bi.baseKey[:0]
	bi.key = bi.key[:0]
	bi.prevOverlap = 0
}

func (bi *blockIterator) seekToFirst() {
	bi.setIdx(0)
}

func (bi *blockIterator) seekToLast() {
	bi.setIdx(len(bi.entryOffsets) - 1)
}

// setIdx 将 bi 指向 block 内索引为 i 的 key
func (bi *blockIterator) setIdx(i int) {
	if i >= len(bi.entryOffsets) || i < 0 {
		bi.err = io.EOF
		return
	}
	bi.idx = i
	bi.err = nil

	// 设置 baseKey
	if len(bi.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(bi.data)
		bi.baseKey = bi.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	if bi.idx+1 == len(bi.entryOffsets) {
		endOffset = len(bi.data)
	} else {
		endOffset = int(bi.entryOffsets[bi.idx+1])
	}
	startOffset := int(bi.entryOffsets[i])

	entryData := bi.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > bi.prevOverlap {
		bi.key = append(bi.key[:bi.prevOverlap], bi.baseKey[bi.prevOverlap:h.overlap]...)
	}
	bi.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	// 用重叠部分和不同部分组成完整的 key
	bi.key = append(bi.key[:h.overlap], diffKey...)

	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOff:])
	bi.val = val.Value
	e := &utils.Entry{
		Key:       bi.key,
		Value:     val.Value,
		ExpiresAt: val.ExpiresAt,
		Meta:      val.Meta,
	}
	bi.item = &Item{e: e}
}

func (bi *blockIterator) Error() error {
	return bi.err
}

func (bi *blockIterator) Next() {
	bi.setIdx(bi.idx + 1)
}

func (bi *blockIterator) Valid() bool {
	return bi.err != io.EOF
}

func (bi *blockIterator) Item() utils.Item {
	return bi.item
}

func (bi *blockIterator) Rewind() bool {
	bi.setIdx(0)
	return true
}

func (bi *blockIterator) Close() error {
	return nil
}
