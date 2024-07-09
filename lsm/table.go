package lsm

import (
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
)

type table struct {
	ss   *file.SSTable
	lm   *levelManager
	fid  uint32
	idxs []byte
}

func openTable(lm *levelManager, tableName string) *table {
	t := &table{
		ss: file.OpenSStable(
			&file.Options{
				FileName: tableName,
				Dir:      lm.opt.WorkDir,
				Flag:     os.O_CREATE | os.O_RDWR,
				MaxSz:    int(lm.opt.SSTableMaxSz),
			}),
	}
	t.idxs = t.ss.Indexs()
	t.lm = lm
	t.fid = utils.FID(tableName)
	return t
}

func (t *table) Serach(key []byte) (entry *codec.Entry, err error) {
	keyStr := string(key)
	idxStr := string(t.idxs)
	idxx := strings.Split(idxStr, ",")
	idx := -1
	for i := 0; i < len(idxx); i += 2 {
		if keyStr == idxx[i] {
			idx, err = strconv.Atoi(idxx[i+1])
			utils.Panic(err)
		}
	}
	if idx == -1 {
		return nil, utils.ErrKeyNotFound
	}
	if block, ok := t.lm.cache.blocks.Get(fmt.Sprintf("%d-%d", t.fid, 0)); ok {
		data, _ := block.([]byte)
		return t.getEntry(key, data, idx)
	}
	var block []byte
	blocks, offsets := t.ss.LoadData()
	if len(blocks) > 0 {
		block = blocks[0]
		t.lm.cache.blocks.Set(fmt.Sprintf("%d-%d", t.fid, offsets[0]), blocks[0])
	}
	return t.getEntry(key, block, idx)
}
func (t *table) getEntry(key, block []byte, idx int) (entry *codec.Entry, err error) {
	if len(block) == 0 {
		return nil, utils.ErrKeyNotFound
	}
	dataStr := string(block)
	blocks := strings.Split(dataStr, ",")
	if idx >= 0 && idx < len(blocks) {
		return &codec.Entry{
			Key:   key,
			Value: []byte(blocks[idx]),
		}, nil
	}
	return nil, utils.ErrKeyNotFound
}
