package codec

import (
	"encoding/binary"
	"reflect"
	"unsafe"
)

type ValuePtr struct {
}

func NewValuePtr(entry *Entry) *ValuePtr {
	return &ValuePtr{}
}

func IsValuePtr(entry *Entry) bool {
	return false
}

func ValuePtrDecode(data []byte) *ValuePtr {
	return nil
}

func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func U32ToBytes(v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return buf[:]
}

func U64ToBytes(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func U32SliceToBytes(u32s []uint32) []byte {
	if len(u32s) == 0 {
		return nil
	}
	var b []byte
	head := (*reflect.SliceHeader)(unsafe.Pointer(&b))
	head.Len = len(u32s) * 4
	head.Cap = head.Len
	head.Data = uintptr(unsafe.Pointer(&u32s[0]))
	return b
}

func BytesToU32Slice(buf []byte) []uint32 {
	if len(buf) == 0 {
		return nil
	}
	var u32s []uint32
	head := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	head.Len = len(buf) / 4
	head.Cap = head.Len
	head.Data = uintptr(unsafe.Pointer(&buf[0]))
	return u32s
}
