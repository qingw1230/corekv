package utils

import (
	"bytes"
	"encoding/binary"
	"math"
)

func ParseKey(key []byte) []byte {
	if len(key) < timestampLen {
		return key
	}
	return key[:len(key)-timestampLen]
}

func ParseTs(key []byte) uint64 {
	if len(key) <= timestampLen {
		return 0
	}
	return math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-timestampLen:])
}

// SameKey 忽略时间戳后缀看 key 是否相等
func SameKey(src, dst []byte) bool {
	if len(src) != len(dst) {
		return false
	}
	return bytes.Equal(ParseKey(src), ParseKey(dst))
}

func KeyWithTs(key []byte, ts uint64) []byte {
	out := make([]byte, len(key)+timestampLen)
	copy(out, key)
	binary.BigEndian.PutUint64(out[len(key):], ts)
	return out
}

func SafeCopy(a, src []byte) []byte {
	return append(a[:0], src...)
}
