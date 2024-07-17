package lsm

import (
	"fmt"
	"math/rand"
	"os"
	"testing"
	"time"

	"github.com/qingw1230/corekv/utils"
	"github.com/stretchr/testify/assert"
)

var (
	opt = &Options{
		WorkDir:             "../work_test",
		SSTableMaxSz:        1024,
		MemTableSize:        1024,
		BlockSize:           1024,
		BloomFalsePositive:  0,
		BaseLevelSize:       10 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize:       2 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  15,
		MaxLevelNum:         7,
		NumCompactors:       2,
	}
)

func TestBase(t *testing.T) {
	clearDir()
	test := func() {
		lsm := buildLSM()
		baseTest(t, lsm, 128)
	}
	runTest(test, 2)
}

func TestRecovery(t *testing.T) {
	test := func() {
		lsm := buildLSM()
		baseTest(t, lsm, 128)
		lsm.Set(buildEntry())
	}
	runTest(test, 1)
}

func TestCompact(t *testing.T) {
	clearDir()
	lsm := buildLSM()
	lsm.StartCompacter()
	test := func() {
		baseTest(t, lsm, 100)
	}
	runTest(test, 10)
}

func buildLSM() *LSM {
	lsm := NewLSM(opt)
	return lsm
}

func buildEntry() *utils.Entry {
	rand.Seed(time.Now().Unix())
	key := []byte(fmt.Sprintf("%s%s", randStr(16), "12345678"))
	value := []byte(randStr(128))
	expiresAt := uint64(time.Now().Add(12*time.Hour).UnixNano() / 1e6)
	return &utils.Entry{
		Key:       key,
		Value:     value,
		ExpiresAt: expiresAt,
	}
}

func baseTest(t *testing.T, lsm *LSM, n int) {
	e := &utils.Entry{
		Key:       []byte("CRTSmI4xYMrGSBtL12345678"),
		Value:     []byte("hImkq95pkCRARFlUoQpCYUiNWYV9lkOd9xiUs0XtFNdOZe5siJVcxjc6j3E5LUng"),
		ExpiresAt: 0,
	}

	lsm.Set(e)
	for i := 1; i < n; i++ {
		lsm.Set(buildEntry())
	}
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	assert.Equal(t, e.Value, v.Value)
}

func runTest(test func(), n int) {
	for i := 0; i < n; i++ {
		test()
	}
}

func randStr(length int) string {
	str := "0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
	bytes := []byte(str)
	result := []byte{}
	rand.Seed(time.Now().UnixNano() + int64(rand.Intn(100)))
	for i := 0; i < length; i++ {
		result = append(result, bytes[rand.Intn(len(bytes))])
	}
	return string(result)
}

func clearDir() {
	_, err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir, os.ModePerm)
}
