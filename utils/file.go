package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"

	"github.com/pkg/errors"
)

func CompareKeys(key1, key2 []byte) int {
	len1 := len(key1)
	len2 := len(key2)
	CondPanic(len1 <= timestampLen || len2 <= timestampLen, fmt.Errorf("%s,%s", string(key1), string(key2)))
	if comp := bytes.Compare(key1[:len1-timestampLen], key2[:len2-timestampLen]); comp != 0 {
		return comp
	}
	return bytes.Compare(key1[len1-timestampLen:], key2[len2-timestampLen:])
}

// VerifyChecksum 验证 data 的校验和是否与 expected 相同
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMissmatch, "actual: %d, expected: %d", actual, expectedU64)
	}
	return nil
}

// CalculateChecksum 计算 data 校验和
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
