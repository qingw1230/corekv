package utils

// Entry 数据节点
type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Meta         byte
	Offset       uint32
	Hlen         int   // WalHeader 的长度
	ValThreshold int64 // 将 value 写入 vlog 的临界大小
}

func newEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}
