package utils

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Seek(key []byte)
	Close() error
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
