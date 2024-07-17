package lsm

import (
	coreCache "github.com/qingw1230/corekv/utils/cache"
)

type cache struct {
	indexs *coreCache.Cache // key fid, value table
	blocks *coreCache.Cache // key fid_blockOffset value block []byte
}

const defaultCacheSize = 1024

func newCache(opt *Options) *cache {
	return &cache{
		indexs: coreCache.NewCache(defaultCacheSize),
		blocks: coreCache.NewCache(defaultCacheSize),
	}
}

func (c *cache) close() error {
	return nil
}

type blockBuffer struct {
	b []byte
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
