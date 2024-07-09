package file

import (
	"os"
	"sync"

	"github.com/qingw1230/corekv/utils"
	"github.com/qingw1230/corekv/utils/codec"
)

// WalFile 预写日志文件结构
type WalFile struct {
	rw *sync.RWMutex
	f  *MmapFile
}

func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &WalFile{f: omf, rw: &sync.RWMutex{}}
}

func (wf *WalFile) Write(entry *codec.Entry) error {
	walData := codec.WalCodec(entry)
	wf.rw.Lock()
	fileData, _, err := wf.f.AllocateSlice(len(walData), 0)
	utils.Panic(err)
	copy(fileData, walData)
	wf.rw.Unlock()
	return nil
}

func (w *WalFile) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return nil
}
