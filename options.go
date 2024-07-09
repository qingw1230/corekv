package corekv

import "github.com/qingw1230/corekv/utils"

type Options struct {
	ValueThreshold int64
	WorkDir        string
	MemTableSize   int64
	SSTableMaxSz   int64
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
