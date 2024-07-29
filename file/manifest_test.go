package file

import (
	"testing"
)

var (
	mfOpt = &Options{
		Dir:   "../work_test",
		MaxSz: 1024,
	}
)

func TestManifestCreate(t *testing.T) {
	clearDir()
	OpenManifestFile(mfOpt)
}

func TestManifestReplay(t *testing.T) {
	OpenManifestFile(mfOpt)
}
