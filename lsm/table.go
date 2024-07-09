package lsm

import "github.com/qingw1230/corekv/file"

type table struct {
	ss   *file.SSTable
	idxs []byte
}

func openTable(opt *Options, tableName string) *table {
	t := &table{
		ss: file.OpenSSTable(
			&file.Options{
				Name: tableName,
				Dir:  opt.WorkDir,
			}),
	}
	t.idxs = t.ss.Indexs()
	return t
}
