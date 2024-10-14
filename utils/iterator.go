package utils

type Iterator interface {
	Next()
	Valid() bool
	Seek(key []byte)
	Item() Item
	Rewind()
	Close() error
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	IsAsc bool
}
