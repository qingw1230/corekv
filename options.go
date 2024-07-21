package corekv

import "github.com/qingw1230/corekv/utils"

type Options struct {
	ValueThreshold      int64 // value 写入 vlog 的临界大小
	WorkDir             string
	MemTableSize        int64
	SSTableMaxSz        int64
	MaxBatchCount       int64 // 最大批量写入个数
	MaxBatchSize        int64 // 最大批量写入大小（以字节为单位）
	ValueLogFileSize    int
	VerifyValueChecksum bool
	ValueLogMaxEntries  uint32
	LogRotatesToFlush   int32
	MaxTableSize        int64
}

func NewDefaultOptions() *Options {
	opt := &Options{
		ValueThreshold: utils.DefaultValueThreshold,
		WorkDir:        "./work_test",
		MemTableSize:   1024,
		SSTableMaxSz:   1 << 30,
	}
	return opt
}
