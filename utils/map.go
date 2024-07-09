package utils

import "sync"

type CoreMap struct {
	m sync.Map
}

func NewMap() *CoreMap {
	return &CoreMap{
		m: sync.Map{},
	}
}

func (c *CoreMap) Get(key interface{}) (interface{}, bool) {
	return c.m.Load(key)
}

func (c *CoreMap) Set(key, value interface{}) {
	c.m.Store(key, value)
}

func (c *CoreMap) Range(f func(key, value interface{}) bool) {
	c.m.Range(f)
}
