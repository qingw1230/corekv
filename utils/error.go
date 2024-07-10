package utils

import (
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
)

var (
	gopath = path.Join(os.Getenv("GOPATH"), "src") + "/"
)

var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("key cannot be empty")
	// ErrReWriteFailure rewrite failure
	ErrReWriteFailure = errors.New("rewrite failure")
	// ErrBadMagic bad magic
	ErrBadMagic = errors.New("bad magic")
	// ErrBadChecksum bad check sum
	ErrBadChecksum = errors.New("bad check sum")
)

// Panic err 不为 nil 则触发 panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

// Err 出错时打印调用栈信息
func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s", location(2, true), err)
	}
	return err
}

func location(deep int, fullPath bool) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}

	if fullPath {
		file = strings.TrimPrefix(file, gopath)
	} else {
		file = filepath.Base(file)
	}
	return file + ":" + strconv.Itoa(line)
}

// CondPanic 根据 condition 和 err 决定是否触发 panic
func CondPanic(condition bool, err error) {
	if condition {
		Panic(err)
	}
}
