package utils

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strconv"
)

var (
	ErrChecksumMissmatch = errors.New("checksum mismatch")

	ErrTruncate = errors.New("do truncate")
	ErrStop     = errors.New("Stop")
)

// Panic err != nil 时触发 panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}

func Panic2(_ interface{}, err error) {
	Panic(err)
}

func Err(err error) error {
	if err != nil {
		fmt.Printf("%s %s\n", location(2), err)
	}
	return err
}

// location 获取调用栈信息，deep 表示调用栈深度
func location(deep int) string {
	_, file, line, ok := runtime.Caller(deep)
	if !ok {
		file = "???"
		line = 0
	}
	file = filepath.Base(file)
	return file + ":" + strconv.Itoa(line)
}

func CondPanic(cond bool, err error) {
	if cond {
		Panic(err)
	}
}
