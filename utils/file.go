package utils

import (
	"bytes"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/pkg/errors"
)

// FID 根据 sst 文件名（00001.sst）获取其 id，不符时返回 0
func FID(name string) uint64 {
	// 获取最后一个元素，去掉前面的路径
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

// FileNameSSTable 用目录和 id 生成 sst 文件全路径
func FileNameSSTable(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

func openDir(path string) (*os.File, error) {
	return os.Open(path)
}

// SyncDir 当创建或删除文件时，必须确保该文件的目录项已同步，以确保在系统崩溃时文件仍可见
func SyncDir(dir string) error {
	f, err := openDir(dir)
	if err != nil {
		return errors.Wrapf(err, "While opening directory: %s", dir)
	}
	err = f.Sync()
	closeErr := f.Close()
	if err != nil {
		return errors.Wrapf(err, "While syncing directory: %s", dir)
	}
	return errors.Wrapf(closeErr, "While closing directory: %s", dir)
}

// LoadIDMap 获取指定文件夹中所有 sst 文件的 id
func LoadIDMap(dir string) map[uint64]struct{} {
	fildInfos, err := ioutil.ReadDir(dir)
	Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fildInfos {
		if info.IsDir() {
			continue
		}
		fileID := FID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}

// CompareKeys 比较 key 大小，必须确保 key 长度大于 8 位
func CompareKeys(key1, key2 []byte) int {
	CondPanic(len(key1) < 8 || len(key2) < 8, fmt.Errorf("%s,%s < 8", key1, key2))
	if cmp := bytes.Compare(key1[:8], key2[:8]); cmp != 0 {
		return cmp
	}
	return bytes.Compare(key1[8:], key2[8:])
}

// VerifyChecksum 验证 data 的校验和是否与 expected 相同
func VerifyChecksum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrChecksumMismatch, "actual: %d, expected: %d", actual, expectedU64)
	}
	return nil
}

// CalculateChecksum 计算 data 的校验和
func CalculateChecksum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
