package utils

import (
	"hash/crc32"
	"os"
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWAITEMANIFEST"
	ManifestDeletionsRatio            = 2
	ManifestDeletionsRewriteThreshold = 1000
	DefaultFileFlag                   = os.O_CREATE | os.O_RDWR | os.O_APPEND
	DefaultFileMode                   = 0666
)

const (
	BitDelete       byte = 1 << 0 // key 被删除时设置
	BitValuePointer byte = 1 << 1 // value 存储到 vlog 文件时设置
)

// codec
var (
	MagicText          = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion       = uint32(1)
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
