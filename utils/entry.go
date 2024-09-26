package utils

// Entry 数据节点
type Entry struct {
	Key   []byte
	Value []byte
}

func newEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}
