package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

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

// FID 获取 sst 文件 id
func FID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, ".sst") {
		return 0
	}
	name = strings.TrimSuffix(name, ".sst")
	id, err := strconv.Atoi(name)
	if err != nil {
		Err(err)
		return 0
	}
	return uint64(id)
}

// FileNameSSTable 根据 dir 和 id 生成 sst 文件名
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

func openDir(path string) (*os.File, error) {
	return os.Open(path)
}

func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "while opening directory: %s", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "while syncing directory: %s", dir)
	}
	return errors.Wrapf(closeErr, "while closing directory: %s", dir)
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
