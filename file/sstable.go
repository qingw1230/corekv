package file

import (
	"encoding/json"
	"io/ioutil"

	"github.com/qingw1230/corekv/utils"
)

// SSTable SST 文件结构
type SSTable struct {
	f      *MockFile
	indexs []byte
	fid    string
}

func OpenSSTable(opt *Options) *SSTable {
	return &SSTable{
		f:   OpenMockFile(opt),
		fid: utils.FID(opt.Name),
	}
}

func (s *SSTable) Indexs() []byte {
	if len(s.indexs) == 0 {
		bv, _ := ioutil.ReadAll(s.f)
		m := make(map[string]interface{}, 0)
		json.Unmarshal(bv, &m)
		if idx, ok := m["idx"]; !ok {
			panic("sst idx is nil")
		} else {
			dataStr, _ := idx.(string)
			s.indexs = []byte(dataStr)
		}
	}
	return s.indexs
}

func (s *SSTable) FID() string {
	return s.fid
}
