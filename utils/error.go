package utils

// Panic err 不为 nil 则触发 panic
func Panic(err error) {
	if err != nil {
		panic(err)
	}
}
