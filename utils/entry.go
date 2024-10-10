package utils

import "encoding/binary"

// Entry 数据节点
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int   // WalHeader 的长度
	ValThreshold int64 // 将 value 写入 vlog 的临界大小
}

func newEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) Entry() *Entry {
	return e
}

// EncodedSize 编码 ValueStruct 所需大小
func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value) + 1 // meta
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return
}

type ValueStruct struct {
	Meta      byte // 用于区分 Value 存的是值指针还是真实的值
	Value     []byte
	ExpiresAt uint64

	Version uint64 // 该字段未被序列化，仅由内部使用
}

func (vs *ValueStruct) EncodedSize() uint32 {
	sz := len(vs.Value) + 1 // meta
	enc := sizeVarint(vs.ExpiresAt)
	return uint32(sz + enc)
}

// EncodeValue 把 vs 编码进 b
// 格式 | Meta | ExpiresAt | Value |
func (vs *ValueStruct) EncodeValue(b []byte) uint32 {
	b[0] = vs.Meta
	sz := binary.PutUvarint(b[1:], vs.ExpiresAt)
	n := copy(b[1+sz:], vs.Value)
	return uint32(1 + sz + n)
}

func (vs *ValueStruct) DecodeValue(b []byte) {
	vs.Meta = b[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(b[1:])
	vs.Value = b[1+sz:]
}
