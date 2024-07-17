package cache

import (
	"container/list"
	"sync"
	"unsafe"

	xxhash "github.com/cespare/xxhash/v2"
)

type Cache struct {
	rw        sync.RWMutex
	lru       *windowLRU
	slru      *segmentedLRU
	door      *BloomFilter
	cnt       *cmSketch                // key 的近似频率
	t         int32                    // 计数器，Get 时增加
	threshold int32                    // 当计数器等于临界值时调用 reset
	data      map[uint64]*list.Element // 保存所有数据
}

// NewCache 创建缓存，由四部分组成 bloom、w-lru、slru、cmsketch
func NewCache(size int) *Cache {
	// w-lru 占总大小 1%
	// slru 占总大小99%
	const lruPct = 1

	lruSz := (lruPct * size) / 100
	if lruSz < 1 {
		lruSz = 1
	}

	slruSz := int(float64(size) * ((100 - lruPct) / 100.0))
	if slruSz < 1 {
		slruSz = 1
	}

	// cold 占 slru 20%
	// hot 占 slru 80%
	slruO := int(0.2 * float64(slruSz))
	if slruO < 1 {
		slruO = 1
	}

	data := make(map[uint64]*list.Element, size)
	return &Cache{
		lru:  newWindowLRU(lruSz, data),
		slru: newSLRU(data, slruO, slruSz-slruO),
		door: newFilter(size, 0.01),
		cnt:  newCmSketch(int64(size)),
		data: data,
	}
}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.rw.Lock()
	defer c.rw.Unlock()
	return c.set(key, value)
}

func (c *Cache) set(key, value interface{}) bool {
	keyHash, conflictHash := c.keyToHash(key)

	item := storeItem{
		stage:    STAGE_WINDOW,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}

	// 先向 w-lru 中插入，w-lru 还没满，插入完成
	eitem, evicted := c.lru.add(item)
	if !evicted {
		return true
	}

	// 有数据被淘汰，看 slru 是否满了，没满直接插入
	victim := c.slru.victim()
	if victim == nil {
		c.slru.add(eitem)
		return true
	}

	if !c.door.Allow(uint32(keyHash)) {
		return true
	}

	// 比较 w-lru 和 slru 淘汰的数据的频率，保留频率多的
	wCnt := c.cnt.Estimate(eitem.key)
	sCnt := c.cnt.Estimate(victim.key)

	if wCnt < sCnt {
		return true
	}

	c.slru.add(eitem)
	return true
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.rw.RLock()
	defer c.rw.RUnlock()
	return c.get(key)
}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	// 计数器到达临界时后，调用 Reset()
	if c.t == c.threshold {
		c.cnt.Reset()
		c.door.reset()
		c.t = 0
	}

	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		c.cnt.Increment(keyHash)
		return nil, false
	}

	item := val.Value.(*storeItem)

	if item.conflict != conflictHash {
		c.cnt.Increment(keyHash)
		return nil, false
	}

	c.cnt.Increment(item.key)

	v := item.value

	// 调用相应部分的 get，提到链表的头部
	if item.stage == STAGE_WINDOW {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}
	return v, true
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.rw.Lock()
	defer c.rw.Unlock()
	return c.del(key)
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}

	item := val.Value.(*storeItem)

	if conflictHash != 0 && (conflictHash != item.conflict) {
		return 0, false
	}

	delete(c.data, keyHash)
	return item.conflict, true
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

// MemHashString is the hash function used by go map, it utilizes available hardware instructions
// (behaves as aeshash if aes instruction is available).
// NOTE: The hash seed changes for every process. So, this cannot be used as a persistent hash.
func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}
