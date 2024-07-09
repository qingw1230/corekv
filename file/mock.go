package file

import (
	"fmt"
	"os"

	"github.com/qingw1230/corekv/utils"
)

type MockFile struct {
	f *os.File
}

type Options struct {
	Name string
	Dir  string
}

func OpenMockFile(opt *Options) *MockFile {
	var err error
	lf := &MockFile{}
	lf.f, err = os.Open(fmt.Sprintf("%s/%s", opt.Dir, opt.Name))
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
