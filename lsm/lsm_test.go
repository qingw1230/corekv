package lsm

import (
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
		{Key: []byte("hello7"), Value: []byte("world"), ExpiresAt: uint64(0)},
	}

	opt := &Options{
		WorkDir: "../work_test",
	}
	levelLive := func() {
		levels := newLevelManager(opt)
		defer func() { _ = levels.close() }()
		imm := &memTable{
			wal: file.OpenWalFile(&file.Options{}),
			sl:  utils.NewSkipList(),
		}
		for _, entry := range entrys {
			imm.set(entry)
		}
		assert.Nil(t, levels.flush(imm))
		v, err := levels.Get([]byte("Hello"))
		assert.Nil(t, err)
		assert.Equal(t, codec.Entry{Value: []byte("Corekv")}.Value, v)
		t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.Value)
		assert.Nil(t, levels.close())
	}
	for i := 0; i < 10; i++ {
		levelLive()
	}
}
