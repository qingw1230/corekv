package file

import (
	"os"
	"testing"

	"github.com/qingw1230/corekv/utils"
)

var (
	walOpt = &Options{
		FileName: "../work_test/wal_test.wal",
		MaxSz:    1024,
	}
)

func TestWALWrite(t *testing.T) {
	clearDir()
	wf := OpenWalFile(walOpt)
	e := utils.BuildEntry()
	wf.Write(e)
}

// clearDir 清空工作目录
func clearDir() {
	_, err := os.Stat("../work_test")
	if err == nil {
		os.RemoveAll("../work_test")
	}
	os.Mkdir("../work_test", os.ModePerm)
}
