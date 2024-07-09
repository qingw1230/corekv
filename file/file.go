package file

type CoreFile interface {
	Read(b []byte) (n int, err error)
	Write(b []byte) (n int, err error)
	Close() error
	Truncature(n int64) error
	ReName(name string) error
}
