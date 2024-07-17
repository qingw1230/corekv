package utils

type Iterator interface {
	Next()
	Valid() bool
	Rewind() // 定位到遍历所需的第一个 key，降序遍历时就是定位到最后
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
