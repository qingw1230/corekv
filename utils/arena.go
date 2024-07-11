package utils

import (
	"log"
	"sync/atomic"
	"unsafe"

	"github.com/pkg/errors"
)

// Arena 一个简易内存池
type Arena struct {
	n   uint32 // 已使用偏移
	buf []byte
}

const MaxNodeSize = int(unsafe.Sizeof(Element{}))

const offsetSize = int(unsafe.Sizeof(uint32(0)))
const nodeAlign = int(unsafe.Sizeof(uint64(0))) - 1

func newArena(n int64) *Arena {
	return &Arena{
		n:   1,
		buf: make([]byte, n),
	}
}

// allocate 申请 sz 字节空间
func (a *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&a.n, sz)

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

// putNode 申请层高为 height 的跳表节点空间
func (a *Arena) putNode(height int) uint32 {
	unusedSize := (defaultMaxLevel - height) * offsetSize
	l := uint32(MaxNodeSize - unusedSize + nodeAlign)
	n := a.allocate(l)
	// TODO(qingw1230): 怎么返回起始地址也对齐的情况，要怎么申请内存
	// m := (n + uint32(nodeAlign)) & ^uint32(nodeAlign)
	return n
}

func (a *Arena) putKey(key []byte) uint32 {
	keySz := uint32(len(key))
	offset := a.allocate(keySz)
	buf := a.buf[offset : offset+keySz]
	AssertTrue(len(key) == copy(buf, key))
	return offset
}

func (a *Arena) putVal(v ValueStruct) uint32 {
	l := v.EncodedSize()
	offset := a.allocate(l)
	v.EncodeValue(a.buf[offset:])
	return offset
}

func (a *Arena) getElement(offset uint32) *Element {
	if offset == 0 {
		return nil
	}
	return (*Element)(unsafe.Pointer(&a.buf[offset]))
}

func (a *Arena) getKey(offset uint32, size uint16) []byte {
	return a.buf[offset : offset+uint32(size)]
}

func (a *Arena) getVal(offset uint32, size uint32) (v ValueStruct) {
	v.DecodeValue(a.buf[offset : offset+size])
	return
}

func (a *Arena) getElementOffset(node *Element) uint32 {
	if node == nil {
		return 0
	}
	return uint32(uintptr(unsafe.Pointer(node)) - uintptr(unsafe.Pointer(&a.buf[0])))
}

func (e *Element) getNextOffset(h int) uint32 {
	return atomic.LoadUint32(&e.levels[h])
}

func (a *Arena) Size() int64 {
	return int64(atomic.LoadUint32(&a.n))
}

func AssertTrue(b bool) {
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}
