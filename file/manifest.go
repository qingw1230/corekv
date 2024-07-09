package file

import (
	"encoding/json"
	"sync"

	"github.com/qingw1230/corekv/utils"
)

type Manifest struct {
	rw  *sync.RWMutex
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
		tables: make([][]*Cell, utils.MaxLevelNum),
		rw:     &sync.RWMutex{},
	}
	mmapFile, err := OpenMmapFile(opt.FileName, opt.Flag, opt.MaxSz)
	utils.Panic(err)
	mf.f = mmapFile
	data := mf.f.Slice(0)
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
	mf.rw.Lock()
	defer mf.rw.Unlock()
	fileData, _, err := mf.f.AllocateSlice(len(data), 0)
	utils.Panic(err)
	copy(fileData, data)
	return err
}
