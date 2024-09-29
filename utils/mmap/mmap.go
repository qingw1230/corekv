package mmap

import "os"

func Mmap(fd *os.File, writable bool, sz int64) ([]byte, error) {
	return mmap(fd, writable, sz)
}

// Mremap munmap and mmap
func Mremap(data []byte, sz int) ([]byte, error) {
	return mremap(data, sz)
}

func Munmap(b []byte) error {
	return munmap(b)
}

func Msync(b []byte) error {
	return msync(b)
}
