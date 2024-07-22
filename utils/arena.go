package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

const (
	offsetSize  = int(unsafe.Sizeof(uint32(0)))     // 一层所需大小
	nodeAlign   = int(unsafe.Sizeof(uint64(0))) - 1 // 节点对齐数
	MaxNodeSize = int(unsafe.Sizeof(node{}))        // 一个跳表节点最大大小
)

// Arena 一个简易内存池，是无锁的
type Arena struct {
	n          uint32 // 已使用偏移
	shouldGrow bool
	buf        []byte
}

func newArena(n int64) *Arena {
	// 开始的 1 字节表示空节点
	return &Arena{
		n:   1,
		buf: make([]byte, n),
	}
}

// allocate 申请 sz 字节空间
func (a *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.n, sz)
	if !a.shouldGrow {
		AssertTrue(int(offset) <= len(a.buf))
		return offset - sz
	}

	// 空间不够时扩容
	if int(offset) > len(a.buf)-MaxNodeSize {
		growBy := uint32(len(a.buf))
		if growBy > 1<<30 {
			growBy = 1 << 30
		}
		if growBy < sz {
			growBy = sz
		}

		newBuf := make([]byte, len(a.buf)+int(growBy))
		AssertTrue(len(a.buf) == copy(newBuf, a.buf))
		a.buf = newBuf
	}

	return offset - sz
}

// size 获取已使用大小
func (a *Arena) size() int64 {
	return int64(atomic.LoadUint32(&a.n))
}

// putNode 为层高为 height 的 node 节点申请空间，返回申请的空间的起始偏移
func (a *Arena) putNode(height int) uint32 {
	unusedSize := (maxHeight - height) * offsetSize
	l := uint32(MaxNodeSize-unusedSize+nodeAlign) & ^uint32(nodeAlign)
	n := a.allocate(l)
	return n
}

// putKey 将 key 放入 arena
func (a *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := a.allocate(keySz)
	buf := a.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

// putVal 将 ValueStruct 放入 arena
func (a *Arena) putVal(v ValueStruct) uint32 {
	l := uint32(v.EncodedSize())
	offset := a.allocate(l)
	v.EncodeValue(a.buf[offset:a.n])
	return offset
}

// getNode 根据 offset 取出在 arena 的 node
func (a *Arena) getNode(offset uint32) *node {
	if offset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getKey(offset uint32, size uint16) []byte {
	return a.buf[offset : offset+uint32(size)]
}

func (a *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(a.buf[offset : offset+size])
	return
}

func (a *Arena) getNodeOffset(nd *node) uint32 {
	if nd == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(nd)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
