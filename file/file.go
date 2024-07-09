package file

import "io"

type Options struct {
	FID      uint32
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

type CoreFile interface {
	Close() error
	NewReader(offset int) io.Reader
	Bytes(off, sz int) ([]byte, error)
	AllocateSlice(sz, offset int) ([]byte, int, error)
	Sync() error
	Delete() error
	Slice(offset int) []byte
}
