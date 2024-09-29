package file

import "io"

type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Flag     int
	MaxSz    int
}

type CoreFile interface {
	AllocateSlice(sz, offset int) ([]byte, int, error)
	Close() error
	Delete() error
	NewReader(offset int) io.Reader
	Bytes(offset, sz int) ([]byte, error)
	Slice(offset int) []byte
	Sync() error
}
