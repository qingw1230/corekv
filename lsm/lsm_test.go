package lsm

import (
	"fmt"
	"os"
	"testing"

	"github.com/qingw1230/corekv/file"
	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
	"github.com/stretchr/testify/assert"
)

func TestLevels(t *testing.T) {
	entrys := []*codec.Entry{
		{Key: []byte("hello0"), Value: []byte("world0"), ExpiresAt: uint64(0)},
		{Key: []byte("hello1"), Value: []byte("world1"), ExpiresAt: uint64(0)},
		{Key: []byte("hello2"), Value: []byte("world2"), ExpiresAt: uint64(0)},
		{Key: []byte("hello3"), Value: []byte("world3"), ExpiresAt: uint64(0)},
		{Key: []byte("hello4"), Value: []byte("world4"), ExpiresAt: uint64(0)},
		{Key: []byte("hello5"), Value: []byte("world5"), ExpiresAt: uint64(0)},
		{Key: []byte("hello6"), Value: []byte("world6"), ExpiresAt: uint64(0)},
		{Key: []byte("hello7"), Value: []byte("world7"), ExpiresAt: uint64(0)},
	}

	opt := &Options{
		WorkDir:      "../work_test",
		SSTableMaxSz: 1 << 30,
		MemTableSize: 1024,
	}
	levelLive := func() {
		levels := newLevelManager(opt)
		defer func() { _ = levels.close() }()
		fileName := fmt.Sprintf("%s/%s", opt.WorkDir, "00001.mem")
		imm := &memTable{
			wal: file.OpenWalFile(&file.Options{FileName: fileName, Dir: opt.WorkDir, Flag: os.O_CREATE | os.O_RDWR, MaxSz: int(opt.SSTableMaxSz)}),
			sl:  utils.NewSkipList(),
		}
		for _, entry := range entrys {
			imm.set(entry)
		}
		assert.Nil(t, levels.flush(imm))
		v, err := levels.Get([]byte("hello7"))
		assert.Nil(t, err)
		assert.Equal(t, []byte("world7"), v.Value)
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)
		assert.Nil(t, levels.close())
	}
	for i := 0; i < 10; i++ {
		levelLive()
	}
}
