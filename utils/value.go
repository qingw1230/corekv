package utils

import (
	"encoding/binary"
	"reflect"
	"time"
	"unsafe"
)

const (
	vptrSize = unsafe.Sizeof(ValuePtr{})
)

type ValuePtr struct {
	Len    uint32
	Offset uint32
	FID    uint32
}

func (v *ValuePtr) Less(o *ValuePtr) bool {
	if o == nil {
		return false
	}
	if v.FID != o.FID {
		return v.FID < o.FID
	}
	if v.Offset != o.Offset {
		return v.Offset < o.Offset
	}
	return v.Len < o.Len
}

func (v *ValuePtr) IsZero() bool {
	return v.FID == 0 && v.Offset == 0 && v.Len == 0
}

func (v ValuePtr) Encode() []byte {
	b := make([]byte, vptrSize)
	*(*ValuePtr)(unsafe.Pointer(&b[0])) = v
	return b
}

func (v *ValuePtr) Decode(b []byte) {
	// 先将 *ValuePtr 转成 *[vptrSize]byte
	// 再将 b 的内容拷贝到 v
	copy((*[vptrSize]byte)(unsafe.Pointer(v))[:], b[:vptrSize])
}

func IsValuePtr(e *Entry) bool {
	return e.Meta&BitValuePointer != 0
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

func ValuePtrCodec(vp *ValuePtr) []byte {
	return []byte{}
}

func RunCallback(cb func()) {
	if cb != nil {
		cb()
	}
}

func IsDeletedOrExpired(meta byte, expiresAt uint64) bool {
	if meta&BitDelete != 0 {
		return true
	}
	if expiresAt == 0 {
		return false
	}
	return expiresAt <= uint64(time.Now().Unix())
}

func DiscardEntry(e, vs *Entry) bool {
	if IsDeletedOrExpired(vs.Meta, vs.ExpiresAt) {
		return true
	}
	if (vs.Meta & BitValuePointer) == 0 {
		return true
	}
	return false
}
