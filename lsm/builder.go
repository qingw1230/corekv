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
)

type tableBuilder struct {
	opt           *Options
	curBlock      *block   // 当前使用的 block
	blockList     []*block // 所有 block 的列表
	keyCount      uint32   // 所有 block key 总数
	keyHashes     []uint32 // 所有 key 的 hash 值
	maxVersion    uint64   // 当前的最大版本号
	sstSize       int64
	staleDataSize int   // 过期数据大小
	estimateSz    int64 // 预估大小
}

// block sst 文件的一个 block 块
// block 结构 | kv_data | offsets | offset_len | checksum | checksum_len |
// kv data 结构 | header | diffkey | expires_at | value |
// header 结构 | overlap | diff |
type block struct {
	offset            int      // 当前 block 在 sst 文件的偏移
	checksum          []byte   // 当前 block 的校验和（kv & offsets）
	chkLen            int      // 校验和长度
	entriesIndexStart int      // entryOffsets 起始位置
	data              []byte   // 存储 block 的所有数据
	baseKey           []byte   // 当前 block 的公共 key，第一个插入的数据作公共 key
	entryOffsets      []uint32 // 各个 key 的偏移
	end               int      // 当前结尾位置
	estimateSz        int64    // 预估大小
}

// buildData builder done 生成的数据结构体
// sst 结构 | block1 | block2 | index_data | index_len | checksum | checksum_len |
// sst index 结构 | block_offsets | bloom_filter | max_version | key_count |
// block offset 结构 | key | offset | len |
type buildData struct {
	blockList []*block // 所有 block 列表
	index     []byte   // 序列化后的索引数据
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
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

const headerSize = uint16(unsafe.Sizeof(header{}))

func newTableBuilderWithSSTSize(opt *Options, size int64) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: size,
	}
}

func newTableBuilder(opt *Options) *tableBuilder {
	return &tableBuilder{
		opt:     opt,
		sstSize: opt.SSTableMaxSz,
	}
}

func (t *tableBuilder) empty() bool {
	return len(t.keyHashes) == 0
}

// finish 结束当前 builder，将数据写入切片
func (t *tableBuilder) finish() []byte {
	bd := t.done()
	buf := make([]byte, bd.size)
	written := bd.Copy(buf)
	utils.CondPanic(written == len(buf), nil)
	return buf
}

// add 将 entry 数据添加到 block 中
func (tb *tableBuilder) add(e *utils.Entry, isStale bool) {
	key := e.Key
	// 检查是否需要重新分配一个新的 block
	if tb.tryFinishBlock(e) {
		if isStale {
			tb.staleDataSize += len(key) + 4 /* len */ + 4 /* offset */
		}
		tb.finishBlock()
		tb.curBlock = &block{
			data: make([]byte, tb.opt.BlockSize),
		}
	}

	val := utils.ValueStruct{Value: e.Value}
	tb.keyHashes = append(tb.keyHashes, utils.Hash(utils.ParseKey(key)))
	if version := utils.ParseTs(key); version > tb.maxVersion {
		tb.maxVersion = version
	}

	var diffKey []byte
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
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

// tryFinishBlock 检查当前块是否已满
func (tb *tableBuilder) tryFinishBlock(e *utils.Entry) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.entryOffsets) <= 0 {
		return false
	}

	entriesOffsetsSize := int64((len(tb.curBlock.entryOffsets)+1)*4 + // offsets
		4 + // offset_len
		8 + // checksum
		4) // checksum_len

	tb.curBlock.estimateSz = int64(tb.curBlock.end) +
		int64(6 /* header size for entry */) + int64(len(e.Key)) + int64(e.EncodedSize()) +
		entriesOffsetsSize // offsets checksum...

	return tb.curBlock.estimateSz > int64(tb.opt.BlockSize)
}

func (t *tableBuilder) AddStaleKey(e *utils.Entry) {
	t.staleDataSize += len(e.Key) + len(e.Value) + 4 /* entry offset */ + 4 /* header size */
	t.add(e, true)
}

func (t *tableBuilder) AddKey(e *utils.Entry) {
	t.add(e, false)
}

func (t *tableBuilder) Close() {
}

// finishBlock 结束当前块
func (tb *tableBuilder) finishBlock() {
	if tb.curBlock == nil || len(tb.curBlock.entryOffsets) == 0 {
		return
	}

	tb.curBlock.entriesIndexStart = tb.curBlock.end

	// 追加所有 key 的偏移情况及其长度
	tb.append(utils.U32SliceToBytes(tb.curBlock.entryOffsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.entryOffsets))))

	// 追加校验和（kv 数据和 offsets）及其长度
	checksum := tb.calculateChecksum(tb.curBlock.data[:tb.curBlock.end])
	tb.append(checksum)
	tb.append(utils.U32ToBytes(uint32(len(checksum))))
	tb.estimateSz += tb.curBlock.estimateSz

	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.curBlock.checksum = checksum
	tb.keyCount += uint32(len(tb.curBlock.entryOffsets))
	tb.curBlock = nil
}

// append 向 t.curBlock.data 追加 data
func (t *tableBuilder) append(data []byte) {
	dst := t.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), errors.New("tableBuilder.append data"))
}

// allocate 申请 need 字节空间
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
	checkSum := utils.CalculateChecksum(data)
	return utils.U64ToBytes(checkSum)
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

// flush 将 builder 数据写入 sst 文件
func (tb *tableBuilder) flush(lm *levelManager, tableName string) (*table, error) {
	bd := tb.done()
	t := &table{
		lm:  lm,
		fid: utils.FID(tableName),
	}
	t.sst = file.OpenSSTable(&file.Options{
		FileName: tableName,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(bd.size),
	})
	buf := make([]byte, bd.size)
	// TODO(qingw1230): 有多次拷贝，需要优化
	written := bd.Copy(buf)
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.sst.Bytes(0, bd.size)
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

// Copy 将 buildData 数据拷贝到 dst
func (bd *buildData) Copy(dst []byte) int {
	var written int
	for _, bl := range bd.blockList {
		written += copy(dst[written:], bl.data[:bl.end])
	}

	written += copy(dst[written:], bd.index)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.index))))
	written += copy(dst[written:], bd.checksum)
	written += copy(dst[written:], utils.U32ToBytes(uint32(len(bd.checksum))))
	return written
}

// done 结束当前 builder，生成 Bloom 和 block index 等数据
func (tb *tableBuilder) done() buildData {
	tb.finishBlock()
	if len(tb.blockList) == 0 {
		return buildData{}
	}

	bd := buildData{
		blockList: tb.blockList,
	}

	var f utils.Filter
	if tb.opt.BloomFalsePositive > 0 {
		bits := utils.BloomBitsPerKey(len(tb.keyHashes), tb.opt.BloomFalsePositive)
		f = utils.NewFilter(tb.keyHashes, bits)
	}

	index, dataSize := tb.buildIndex(f)
	checksum := tb.calculateChecksum(index)
	bd.index = index
	bd.checksum = checksum
	bd.size = int(dataSize) + len(index) + len(checksum) + 4 + 4 // index_len & checksum_len
	return bd
}

// buildIndex 使用 tb 数据构建它的索引
func (tb *tableBuilder) buildIndex(bloom []byte) ([]byte, uint32) {
	tableIndex := &pb.TableIndex{}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.writeBlockOffsets(tableIndex)

	var dataSize uint32
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].end)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	return data, dataSize
}

// writeBlockOffsets 将 t.blockList 信息写入 []*pb.BlockOffset
func (t *tableBuilder) writeBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range t.blockList {
		offset := t.writeBlockOffset(bl, startOffset)
		offsets = append(offsets, offset)
		startOffset += uint32(bl.end)
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

// verifyChecksum 验证 block kv_data offsets offset_len 的校验和
func (b *block) verifyChecksum() error {
	return utils.VerifyChecksum(b.data, b.checksum)
}

type blockIterator struct {
	block        *block // 当前指向的 block 块
	data         []byte // block 块的 KV 数据部分
	idx          int    // key 在 entryOffsets 的索引
	err          error  // 记录错误
	baseKey      []byte // 公共 key
	key          []byte // 指向的 key
	val          []byte
	entryOffsets []uint32 // block 内各 key 的偏移

	tableID uint64 // 当前 block 所属 sst 文件
	blockID int    // 在所属 sst 文件的索引

	prevOverlap uint16 // 上一个 key 重叠部分长度

	item utils.Item // 保存数据
}

// setBlock 将 it 指向指定 block
func (it *blockIterator) setBlock(b *block) {
	it.block = b
	it.data = b.data[:b.entriesIndexStart]
	it.idx = 0
	it.err = nil
	it.baseKey = it.baseKey[:0]
	it.key = it.key[:0]
	it.entryOffsets = b.entryOffsets
	it.prevOverlap = 0
}

// seek 在 block 块中查找指定 key，找不到时 it.err = io.EOF
func (it *blockIterator) seek(key []byte) {
	it.err = nil
	startIndex := 0

	foundEntryIdx := sort.Search(len(it.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		it.setIdx(idx)
		return utils.CompareKeys(it.key, key) >= 0
	})
	it.setIdx(foundEntryIdx)
}

func (it *blockIterator) seekToFirst() {
	it.setIdx(0)
}

func (it *blockIterator) seekToLast() {
	it.setIdx(len(it.entryOffsets) - 1)
}

// setIdx 将 it 指向索引为 i 的 key，并更新相关变量
func (it *blockIterator) setIdx(i int) {
	it.idx = i
	if i >= len(it.entryOffsets) || i < 0 {
		it.err = io.EOF
		return
	}
	it.err = nil

	// 设置 baseKey
	if len(it.baseKey) == 0 {
		var baseHeader header
		baseHeader.decode(it.data)
		it.baseKey = it.data[headerSize : headerSize+baseHeader.diff]
	}

	var endOffset int
	if it.idx+1 == len(it.entryOffsets) {
		endOffset = len(it.data)
	} else {
		endOffset = int(it.entryOffsets[it.idx+1])
	}
	startOffset := int(it.entryOffsets[i])

	// 取出 | header | diffkey | expires_at | value |
	entryData := it.data[startOffset:endOffset]
	var h header
	h.decode(entryData)
	if h.overlap > it.prevOverlap {
		it.key = append(it.key[:it.prevOverlap], it.baseKey[it.prevOverlap:h.overlap]...)
	}

	it.prevOverlap = h.overlap
	valueOff := headerSize + h.diff
	diffKey := entryData[headerSize:valueOff]
	// 用重叠部分和不同部分组成完整的 key
	it.key = append(it.key[:h.overlap], diffKey...)
	e := utils.NewEntry(it.key, nil)
	val := &utils.ValueStruct{}
	val.DecodeValue(entryData[valueOff:])
	it.val = val.Value
	e.ExpiresAt = val.ExpiresAt
	e.Value = val.Value
	it.item = &Item{e: e}
}

func (it *blockIterator) Error() error {
	return it.err
}

// Next 指向当前 block 的下一个 key
func (it *blockIterator) Next() {
	it.setIdx(it.idx + 1)
}

func (it *blockIterator) Valid() bool {
	return it.err != io.EOF
}

// Rewind 重新指向当前 block 的第一个 key
func (it *blockIterator) Rewind() bool {
	it.setIdx(0)
	return true
}

func (it *blockIterator) Item() utils.Item {
	return it.item
}

func (it *blockIterator) Close() error {
	return nil
}
