package iterator

import "github.com/qingw1230/corekv/utils/codec"

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Seek(key []byte)
	Close() error
}

type Item interface {
	Entry() *codec.Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
