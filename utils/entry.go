package utils

import (
	"encoding/binary"
	"time"
)

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Version      uint64
	Offset       uint32
	Hlen         int // Length of the header.
	ValThreshold int64
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

type ValueStruct struct {
	Value     []byte
	ExpiresAt uint64
}

// EncodedSize 获取编码 Value 和 ExpiresAt 所需字节数
func (v *ValueStruct) EncodedSize() uint32 {
	sz := len(v.Value)
	enc := sizeVarint(v.ExpiresAt)
	return uint32(sz + enc)
}

// EncodeValue 将 ExpiresAt 和 Value 编码进 buf
func (v *ValueStruct) EncodeValue(buf []byte) uint32 {
	sz := binary.PutUvarint(buf[:], v.ExpiresAt)
	n := copy(buf[sz:], v.Value)
	return uint32(sz + n)
}

// DecodeValue 从 buf 中解码出 ExpiresAt 和 Value
func (v *ValueStruct) DecodeValue(buf []byte) {
	var sz int
	v.ExpiresAt, sz = binary.Uvarint(buf)
	v.Value = buf[sz:]
}

func (e *Entry) Entry() *Entry {
	return e
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
	sz := len(e.Value)
	enc := sizeVarint(e.ExpiresAt)
	return uint32(sz + enc)
}
