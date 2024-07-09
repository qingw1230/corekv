package file

import "github.com/qingw1230/corekv/utils/codec"

// WalFile 预写日志文件结构
type WalFile struct {
	f *MockFile
}

func OpenWalFile(opt *Options) *WalFile {
	return &WalFile{
		f: OpenMockFile(opt),
	}
}

func (w *WalFile) Write(entry *codec.Entry) error {
	walData := codec.WalCodec(entry)
	_, err := w.f.Write(walData)
	return err
}

func (w *WalFile) Close() error {
	if err := w.f.Close(); err != nil {
		return err
	}
	return nil
}
