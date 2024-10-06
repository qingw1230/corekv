package utils

type Iterator interface {
	Next()
	Valid() bool
	Item() Item
	Rewind()
	Seek(key []byte)
	Close() error
}

type Item interface {
	Entry() *Entry
}

type Options struct {
	IsAsc bool
}
