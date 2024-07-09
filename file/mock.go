package file

import (
	"fmt"
	"os"

	"github.com/qingw1230/corekv/utils"
)

type MockFile struct {
	f   *os.File
	opt *Options
}

type Options struct {
	Name string
	Dir  string
}

func OpenMockFile(opt *Options) *MockFile {
	var err error
	lf := &MockFile{}
	lf.opt = opt
	lf.f, err = os.OpenFile(fmt.Sprintf("%s/%s", opt.Dir, opt.Name), os.O_CREATE|os.O_RDWR, 0666)
	utils.Panic(err)
	return lf
}

func (m *MockFile) Read(bytes []byte) (int, error) {
	return m.f.Read(bytes)
}

func (m *MockFile) Write(bytes []byte) (int, error) {
	return m.f.Write(bytes)
}

func (m *MockFile) Close() error {
	if err := m.f.Close(); err != nil {
		return err
	}
	return nil
}

func (lf *MockFile) Truncature(n int64) error {
	return lf.f.Truncate(n)
}

func (lf *MockFile) ReName(name string) error {
	err := os.Rename(fmt.Sprintf("%s/%s", lf.opt.Dir, lf.opt.Name), fmt.Sprintf("%s/%s", lf.opt.Dir, name))
	lf.opt.Name = name
	return err
}
