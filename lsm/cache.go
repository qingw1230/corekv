package lsm

import "github.com/qingw1230/corekv/utils"

type cache struct {
	indexs *utils.CoreMap
	blocks *utils.CoreMap
}

type blockBuffer struct {
	b []byte
}

func newCache(opt *Options) *cache {
	return &cache{
		indexs: utils.NewMap(),
		blocks: utils.NewMap(),
	}
}

func (c *cache) addIndex(fid string, t *table) {
	c.indexs.Set(fid, t)
}

func (c *cache) close() error {
	return nil
}
