package utils

import "errors"

var (
	// ErrKeyNotFound is returned when key isn't found on a txn.Get.
	ErrKeyNotFound = errors.New("Key not found")
	// ErrEmptyKey is returned if an empty key is passed on an update function.
	ErrEmptyKey = errors.New("Key cannot be empty")
)

// Panic err 不为 nil 则触发 panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
