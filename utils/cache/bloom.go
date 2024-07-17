package cache

import "math"

type Filter []byte

type BloomFilter struct {
	bitmap Filter
	k      uint8 // 哈希函数个数
}

func newFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitsPerKey := bloomBitsPerKey(numEntries, falsePositive)
	return initFilter(numEntries, bitsPerKey)
}

func initFilter(numEntries int, bitsPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}

	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	bf.k = uint8(k)

	nBits := numEntries * int(bitsPerKey)
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	filter := make([]byte, nBytes+1)

	filter[nBytes] = uint8(k)

	bf.bitmap = filter
	return bf
}

func (b *BloomFilter) MayContainKey(k []byte) bool {
	return b.MayContain(Hash(k))
}

func (b *BloomFilter) MayContain(h uint32) bool {
	if b.Len() < 2 {
		return false
	}
	k := b.k
	if k > 30 {
		return true
	}
	nBits := uint32(8 * (b.Len() - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if b.bitmap[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (b *BloomFilter) Len() int32 {
	return int32(len(b.bitmap))
}

func (b *BloomFilter) InsertKey(k []byte) bool {
	return b.Insert(Hash(k))
}

func (b *BloomFilter) Insert(h uint32) bool {
	k := b.k
	if k > 30 {
		return true
	}
	nBits := uint32(8 * (b.Len() - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % uint32(nBits)
		b.bitmap[bitPos/8] |= 1 << (bitPos % 8)
		h += delta
	}
	return true
}

// AllowKey 返回之前是否存在，不存在时插入
func (b *BloomFilter) AllowKey(k []byte) bool {
	if b == nil {
		return true
	}
	already := b.MayContainKey(k)
	if !already {
		b.InsertKey(k)
	}
	return already
}

// Allow 返回之前是否存在，不存在时插入
func (b *BloomFilter) Allow(h uint32) bool {
	if b == nil {
		return true
	}
	already := b.MayContain(h)
	if !already {
		b.Insert(h)
	}
	return already
}

// reset 清空所有位
func (b *BloomFilter) reset() {
	if b == nil {
		return
	}
	for i := range b.bitmap {
		b.bitmap[i] = 0
	}
}

// bloomBitsPerKey 根据误报率和元素个数计算 bitsPerKey
func bloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

// Hash Murmur hash
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
