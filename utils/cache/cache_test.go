package cache

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestCacheSLRUHot(t *testing.T) {
	cache := NewCache(100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, _ := cache.Get(key)
		assert.Equal(t, val, res)
	}

	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key10%d", i)
		val := fmt.Sprintf("val10%d", i)
		cache.Set(key, val)
	}

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, _ := cache.Get(key)
		assert.Equal(t, val, res)
	}
}

func TestCacheBasicCRUD(t *testing.T) {
	cache := NewCache(100)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		cache.Set(key, val)
	}

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key%d", i)
		val := fmt.Sprintf("val%d", i)
		res, ok := cache.Get(key)
		if ok {
			assert.Equal(t, val, res)
			continue
		}
		assert.Equal(t, res, nil)
	}
}
