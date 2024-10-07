package file

import (
	"bytes"
	"os"
	"testing"

	"github.com/qingw1230/corekv/utils"
	"github.com/stretchr/testify/assert"
)

var (
	walOpt = &Options{
		FileName: "../work_test/wal_test.wal",
		Dir:      "../work_test",
		MaxSz:    1024,
	}
)

func TestWAL_Write(t *testing.T) {
	clearDir(walOpt)
	wf := OpenWalFile(walOpt)
	e := utils.BuildEntry()
	wf.Write(e)
}

func TestCheckWAL(t *testing.T) {
	clearDir(walOpt)
	wf := OpenWalFile(walOpt)
	entries := utils.GenerateEntries(100000)
	for _, e := range entries {
		wf.Write(e)
	}

	readEntries := make([]*utils.Entry, 0)
	logEntry := func(e *utils.Entry, vp *utils.ValuePtr) error {
		readEntries = append(readEntries, e)
		return nil
	}
	wf.Iterator(true, 0, logEntry)

	for i := 0; i < len(entries); i++ {
		assert.Equal(t, 0, bytes.Compare(entries[i].Key, readEntries[i].Key))
		assert.Equal(t, 0, bytes.Compare(entries[i].Value, readEntries[i].Value))
	}
}

func clearDir(opt *Options) {
	_, err := os.Stat(opt.Dir)
	if err == nil {
		os.RemoveAll(opt.Dir)
	}
	os.Mkdir(opt.Dir, os.ModePerm)
}
