package utils

import (
	"hash/crc32"
	"math"
	"os"
)

const (
	MaxLevelNum           = 7
	DefaultValueThreshold = 1024
)

// file
const (
	ManifestFilename                  = "MANIFEST"
	ManifestRewriteFilename           = "REWRITEMANIFEST"
	ManifestDeletionsRewriteThreshold = 10000
	ManifestDeletionsRatio            = 10
	DefaultFileFlag                   = os.O_RDWR | os.O_CREATE | os.O_APPEND
	DefaultFileMode                   = 0666
	MaxValueLogSize                   = 10 << 20
	datasyncFileFlag                  = 0x0
	// MaxHeaderSize 基于可变长编码，其最可能的长度
	MaxHeaderSize            = 21
	VlogHeaderSize           = 0
	MaxVlogFileSize   uint32 = math.MaxUint32
	Mi                int64  = 1 << 20
	KVWriteChCapacity        = 1000
)

const (
	BitDelete       byte = 1 << 0 // key 被删除时设置
	BitValuePointer byte = 1 << 1 // value 存储到 vlog 文件时设置
)

// codec
var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable 是一个 CRC32 多项式表
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)
