package file

import (
	"sync/atomic"
	"testing"

	"github.com/qingw1230/corekv/utils"
)

var (
	manifestOpt = &Options{
		FileName: utils.ManifestFilename,
		Dir:      "../work_test",
	}
)

func TestOpenManifestFile(t *testing.T) {
	clearDir(manifestOpt)
	mf, err := OpenManifestFile(manifestOpt)
	utils.Panic(err)
	mf.AnalyzaManifest()
}

func TestOpenManifestFile2(t *testing.T) {
	TestManifestAddData(t)

	mf, err := OpenManifestFile(manifestOpt)
	utils.Panic(err)
	mf.AnalyzaManifest()
}

func TestManifestAddData(t *testing.T) {
	clearDir(manifestOpt)
	mf, err := OpenManifestFile(manifestOpt)
	utils.Panic(err)

	id, cnt := uint64(0), 100
	for i := 0; i < cnt; i++ {
		num := atomic.AddUint64(&id, 1)
		err := mf.AddTableMeta(utils.Intn(7)+1, &TableMeta{
			ID:       num,
			Checksum: []byte{'m', 'o', 'c', 'k'},
		})
		utils.Panic(err)
	}

	// mf.AnalyzaManifest()
}
