package file

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"strings"

	"github.com/qingw1230/corekv/iterator"
	"github.com/qingw1230/corekv/utils"
)

// SSTable SST 文件结构
type SSTable struct {
	f      *MockFile
	maxKey []byte
	minKey []byte
	indexs []byte
	fid    string
}

func OpenSStable(opt *Options) *SSTable {
	return &SSTable{f: OpenMockFile(opt), fid: utils.FID(opt.Name)}
}

func (ss *SSTable) Indexs() []byte {
	if len(ss.indexs) == 0 {
		bv, _ := ioutil.ReadAll(ss.f)
		m := make(map[string]interface{}, 0)
		json.Unmarshal(bv, &m)
		if idx, ok := m["idx"]; !ok {
			panic("sst idx is nil")
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

func (ss *SSTable) FID() string {
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
	if err != nil {
		return err
	}
	if _, err := ss.f.Write(bData); err != nil {
		return err
	}
	ss.indexs = []byte(ssData["idx"])
	return nil
}

func (ss *SSTable) LoadData() (blocks [][]byte, offsets []int) {
	ss.f.f.Seek(0, io.SeekStart)
	bv, err := ioutil.ReadAll(ss.f)
	utils.Panic(err)
	m := make(map[string]interface{}, 0)
	json.Unmarshal(bv, &m)
	if data, ok := m["data"]; !ok {
		panic("sst data is nil")
	} else {
		dd := data.(string)
		blocks = append(blocks, []byte(dd))
		offsets = append(offsets, 0)
	}
	return blocks, offsets
}
