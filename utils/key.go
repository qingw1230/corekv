package utils

import (
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
