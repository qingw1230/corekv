package utils

import (
	"bytes"
	"fmt"
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
