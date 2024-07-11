package utils

import (
	"hash/crc32"
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
)

// codec
var (
	MagicText    = [4]byte{'H', 'A', 'R', 'D'}
	MagicVersion = uint32(1)
	// CastagnoliCrcTable 是一个 CRC32 多项式表
	CastagnoliCrcTable     = crc32.MakeTable(crc32.Castagnoli)
	MaxHeaderSize      int = 21
)
