package file

import (
	"bufio"
	"encoding/csv"
	"io"

	"github.com/qingw1230/corekv/utils"
)

type Manifest struct {
	f CoreFile
	// tables l0-l7 各层的 SST 文件名
	tables [][]string
}

func (m *Manifest) Close() error {
	if err := m.f.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Manifest) Tables() [][]string {
	return m.tables
}

func OpenManifest(opt *Options) *Manifest {
	mf := &Manifest{
		f:      OpenMockFile(opt),
		tables: make([][]string, utils.MaxLevelNum),
	}
	reader := csv.NewReader(bufio.NewReader(mf.f))

	level := 0
	for {
		if level > utils.MaxLevelNum {
			break
		}
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			panic(err)
		}
		if len(mf.tables[level]) == 0 {
			mf.tables[level] = make([]string, len(line))
		}
		for j, tableName := range line {
			mf.tables[level][j] = tableName
		}
		level++
	}
	return mf
}
