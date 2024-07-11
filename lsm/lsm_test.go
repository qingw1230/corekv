package lsm

import (
	"os"
	"testing"

	"github.com/qingw1230/corekv/utils"
	"github.com/stretchr/testify/assert"
)

var (
	entrys = []*utils.Entry{
		{Key: []byte("hello0_12345678"), Value: []byte("world0"), ExpiresAt: uint64(0)},
		{Key: []byte("hello1_12345678"), Value: []byte("world1"), ExpiresAt: uint64(0)},
		{Key: []byte("hello2_12345678"), Value: []byte("world2"), ExpiresAt: uint64(0)},
		{Key: []byte("hello3_12345678"), Value: []byte("world3"), ExpiresAt: uint64(0)},
		{Key: []byte("hello4_12345678"), Value: []byte("world4"), ExpiresAt: uint64(0)},
		{Key: []byte("hello5_12345678"), Value: []byte("world5"), ExpiresAt: uint64(0)},
		{Key: []byte("hello6_12345678"), Value: []byte("world6"), ExpiresAt: uint64(0)},
		{Key: []byte("hello7_12345678"), Value: []byte("world7"), ExpiresAt: uint64(0)},
	}
	opt = &Options{
		WorkDir:            "../work_test",
		SSTableMaxSz:       1024,
		MemTableSize:       1024,
		BlockSize:          1024,
		BloomFalsePositive: 0.01,
	}
)

func TestFlushBase(t *testing.T) {
	createDir(t, opt.WorkDir)
	lsm := buildCase()
	defer cleanDir(t, opt.WorkDir)
	test := func() {
		assert.Nil(t, lsm.levels.flush(lsm.memTable))
		baseTest(t, lsm)
	}
	runTest(test, 2)
}

func TestRecoveryBase(t *testing.T) {
	createDir(t, opt.WorkDir)
	defer cleanDir(t, opt.WorkDir)
	buildCase()
	test := func() {
		lsm := NewLSM(opt)
		baseTest(t, lsm)
	}
	runTest(test, 1)
}

func buildCase() *LSM {
	lsm := NewLSM(opt)
	for _, entry := range entrys {
		lsm.Set(entry)
	}
	return lsm
}

func baseTest(t *testing.T, lsm *LSM) {
	v, err := lsm.Get([]byte("hello7_12345678"))
	assert.Nil(t, err)
	assert.Equal(t, []byte("world7"), v.Value)
	t.Logf("levels.Get key=%s, value=%s, expiresAt=%d", v.Key, v.Value, v.ExpiresAt)
}

func runTest(test func(), n int) {
	for i := 0; i < n; i++ {
		test()
	}
}

func cleanDir(t *testing.T, dir string) {
	assert.Nil(t, os.RemoveAll(dir))
}

func createDir(t *testing.T, dir string) {
	assert.Nil(t, os.Mkdir(dir, 0755))
}
