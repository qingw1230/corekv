package utils

// Copy 创建一个切片的副本
func Copy(a []byte) []byte {
	b := make([]byte, len(a))
	copy(b, a)
	return b
}
