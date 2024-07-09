package file

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"sync"

	"github.com/qingw1230/corekv/iterator"
	"github.com/qingw1230/corekv/utils"
)

// SSTable SST 文件结构
type SSTable struct {
	rw     *sync.RWMutex
	f      *MmapFile
	maxKey []byte
	minKey []byte
	indexs []byte
	fid    uint32
}

func OpenSStable(opt *Options) *SSTable {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{f: omf, fid: opt.FID, rw: &sync.RWMutex{}}
}

func (ss *SSTable) Indexs() []byte {
	if len(ss.indexs) == 0 {
		ss.rw.RLock()
		bv := ss.f.Slice(0)
		ss.rw.RUnlock()
		m := make(map[string]interface{}, 0)
		json.Unmarshal(bv, &m)
		if idx, ok := m["idx"]; !ok {
			return []byte{}
		} else {
			dataStr, _ := idx.(string)
			ss.indexs = []byte(dataStr)
			tmp := strings.Split(dataStr, ",")
			ss.maxKey = []byte(tmp[len(tmp)-1])
			ss.minKey = []byte(tmp[0])
		}
	}
	return ss.indexs
}

func (ss *SSTable) MaxKey() []byte {
	return ss.maxKey
}

func (ss *SSTable) MinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) FID() uint32 {
	return ss.fid
}

func (ss *SSTable) SaveSkipListToSSTable(sl *utils.SkipList) error {
	iter := sl.NewIterator(&iterator.Options{})
	indexs, datas, idx := make([]string, 0), make([]string, 0), 0
	for iter.Rewind(); iter.Valid(); iter.Next() {
		item := iter.Item().Entry()
		indexs = append(indexs, string(item.Key))
		indexs = append(indexs, fmt.Sprintf("%d", idx))
		datas = append(datas, string(item.Value))
		idx++
	}
	ssData := make(map[string]string, 0)
	ssData["idx"] = strings.Join(indexs, ",")
	ssData["data"] = strings.Join(datas, ",")
	bData, err := json.Marshal(ssData)
	utils.Err(err)
	ss.rw.Lock()
	fileData, _, err := ss.f.AllocateSlice(len(bData), 0)
	utils.Panic(err)
	copy(fileData, bData)
	ss.rw.Unlock()
	ss.indexs = []byte(ssData["idx"])
	return nil
}

func (ss *SSTable) LoadData() (blocks [][]byte, offsets []int) {
	ss.rw.RLock()
	fileData := ss.f.Slice(0)
	ss.rw.RUnlock()
	m := make(map[string]interface{}, 0)
	json.Unmarshal(fileData, &m)
	if data, ok := m["data"]; !ok {
		panic("sst data is nil")
	} else {
		dd := data.(string)
		blocks = append(blocks, []byte(dd))
		offsets = append(offsets, 0)
	}
	return blocks, offsets
}
