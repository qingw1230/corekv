package file

import (
	"encoding/json"
	"fmt"
	"io/ioutil"

	"github.com/qingw1230/corekv/utils"
)

type Manifest struct {
	opt *Options
	f   CoreFile
	// tables l0-l7 各层的 SST 文件名
	tables [][]*Cell
}

// Cell 一行 Manifest 数据的封装
type Cell struct {
	SSTName string
}

func (m *Manifest) Close() error {
	if err := m.f.Close(); err != nil {
		return err
	}
	return nil
}

func (m *Manifest) Tables() [][]*Cell {
	return m.tables
}

func OpenManifest(opt *Options) *Manifest {
	mf := &Manifest{
		opt:    opt,
		f:      OpenMockFile(opt),
		tables: make([][]*Cell, utils.MaxLevelNum),
	}

	data, err := ioutil.ReadAll(mf.f)
	utils.Panic(err)
	if len(data) == 0 {
		return mf
	}
	tables := make([][]string, 0)
	utils.Panic(json.Unmarshal(data, &tables))
	for i, ts := range tables {
		mf.tables[i] = make([]*Cell, 0)
		for _, name := range ts {
			mf.tables[i] = append(mf.tables[i], &Cell{SSTName: name})
		}
	}
	return mf
}

func (mf *Manifest) AppendSST(levelNum int, cell *Cell) (err error) {
	mf.tables[levelNum] = append(mf.tables[levelNum], cell)
	err = mf.f.Truncature(0)
	if err != nil {
		return err
	}
	res := make([][]string, len(mf.tables))
	for i, cells := range mf.tables {
		res[i] = make([]string, 0)
		for _, cell := range cells {
			res[i] = append(res[i], cell.SSTName)
		}
	}
	data, err := json.Marshal(res)
	if err != nil {
		return err
	}
	ioutil.WriteFile(fmt.Sprintf("%s/%s", mf.opt.Dir, mf.opt.Name), data, 0666)
	return err
}
