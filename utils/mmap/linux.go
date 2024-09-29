package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

// mmap 使用 mmap 系统调用来 memory-map 一个文件
func mmap(fd *os.File, writable bool, sz int64) ([]byte, error) {
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	// MAP_SHARED 修改会同步到磁盘的文件上
	return unix.Mmap(int(fd.Fd()), 0, int(sz), mtype, unix.MAP_SHARED)
}

func mremap(data []byte, sz int) ([]byte, error) {
	const MREMAP_MAYMOVE = 0x1

	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	mmapAddr, _, errno := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,
		uintptr(header.Len),
		uintptr(sz),
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if errno != 0 {
		return nil, errno
	}

	header.Data = mmapAddr
	header.Cap = sz
	header.Len = sz
	return data, nil
}

// munmap 用于取消之前的映射
func munmap(data []byte) error {
	if len(data) == 0 && len(data) != cap(data) {
		return unix.EINVAL
	}
	// unix.Munmap 维护了一个内部列表，记录了所有映射的地址，只有地址在该列表中时才会调用 munmap
	// 如果我们使用 mremap，这个列表不会更新，因此需要自己调用 munmap
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&data)),
		uintptr(len(data)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func msync(b []byte) error {
	return unix.Msync(b, unix.MS_SYNC)
}
