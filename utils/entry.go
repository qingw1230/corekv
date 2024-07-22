package utils

import (
	"encoding/binary"
	"time"
)

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Version      uint64
	Offset       uint32
	Hlen         int   // Length of the header.
	ValThreshold int64 // 将 value 写入 vlog 的临界大小
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

type ValueStruct struct {
	Meta      byte   // 标识是否为值指针
	Value     []byte // 存储实际数据或指示位置的值指针
	ExpiresAt uint64
	Version   uint64
}

// EncodedSize 获取编码 ValueStruct 所需字节数
func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value) + 1 // Meta
	enc := sizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

// EncodeValue 将 ValueStruct 编码进 buf
func (v *ValueStruct) EncodeValue(buf []byte) uint32 {
	buf[0] = v.Meta
	sz := binary.PutUvarint(buf[1:], v.ExpiresAt)
	n := copy(buf[1+sz:], v.Value)
	return uint32(1 + sz + n)
}

// DecodeValue 从 buf 中解码出 ValueStruct
func (v *ValueStruct) DecodeValue(buf []byte) {
	v.Meta = buf[0]
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(buf[1:])
	v.Value = buf[1+sz:]
}

func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) IsDeletedOrExpired() bool {
	if e.Value == nil {
		return true
	}

	if e.ExpiresAt == 0 {
		return false
	}

	return e.ExpiresAt <= uint64(time.Now().Unix())
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// Size 获取 Entry Key 和 Value 的总长度
func (e *Entry) Size() int64 {
	return int64(len(e.Key) + len(e.Value))
}

// sizeVarint 计算 uint64 经过编码后占用的字节数
func sizeVarint(x uint64) int {
	var n int
	for {
		n++
		x >>= 7
		if x == 0 {
			break
		}
	}
	return n
}

func (e *Entry) EncodedSize() uint32 {
	sz := len(e.Value) + 1
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}

// EstimateSize 预估将 Entry 编码进 sst 所需大小
func (e *Entry) EstimateSize(threshold int) int {
	if len(e.Value) < threshold {
		return len(e.Key) + len(e.Value) + 1
	}
	return len(e.Key) + 12 + 1 // 12 for ValuePonter, 1 for Meta
}

// Header 用在 vlog 文件作为 Entry 的头
type Header struct {
	KLen      uint32
	VLen      uint32
	ExpiresAt uint64
	Meta      byte
}

// Encode 将 Header 编码进 out
// Header 格式 | Meta | KLen | VLen | ExpiresAt |
func (h Header) Encode(out []byte) int {
	out[0] = h.Meta
	index := 1
	index += binary.PutUvarint(out[index:], uint64(h.KLen))
	index += binary.PutUvarint(out[index:], uint64(h.VLen))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}

// Decode 从 buf 中解码出 Header
func (h *Header) Decode(buf []byte) int {
	h.Meta = buf[0]
	index := 1
	klen, cnt := binary.Uvarint(buf[index:])
	h.KLen = uint32(klen)
	index += cnt
	vlen, cnt := binary.Uvarint(buf[index:])
	h.VLen = uint32(vlen)
	index += cnt
	h.ExpiresAt, cnt = binary.Uvarint(buf[index:])
	return index + cnt
}

func (h *Header) DecodeFrom(reader *HashReader) (int, error) {
	var err error
	h.Meta, err = reader.ReadByte()
	if err != nil {
		return 0, err
	}
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.VLen = uint32(vlen)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}
