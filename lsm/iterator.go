package lsm

import (
	"github.com/qingw1230/corekv/iterator"
	"github.com/qingw1230/corekv/utils/codec"
)

type Iterator struct {
	it    iterator.Item
	iters []iterator.Iterator
}

type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}

func (l *LSM) NewIterator(opt *iterator.Options) iterator.Iterator {
	iter := &Iterator{}
	iter.iters = make([]iterator.Iterator, 0)
	iter.iters = append(iter.iters, l.memTable.NewIterator(opt))
	for _, imm := range l.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, l.levels.NewIterator(opt))
	return iter
}

func (iter *Iterator) Next() {
	iter.iters[0].Next()
}

func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}

func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}

func (iter *Iterator) Item() iterator.Item {
	return iter.iters[0].Item()
}

func (iter *Iterator) Close() error {
	return nil
}

func (iter *Iterator) Seek(key []byte) {
}

type memIterator struct {
	innerIter iterator.Iterator
}

func (m *memTable) NewIterator(opt *iterator.Options) iterator.Iterator {
	return &memIterator{
		innerIter: m.sl.NewIterator(opt),
	}
}

func (iter *memIterator) Next() {
	iter.innerIter.Next()
}

func (iter *memIterator) Valid() bool {
	return iter.innerIter.Valid()
}

func (iter *memIterator) Rewind() {
	iter.innerIter.Rewind()
}

func (iter *memIterator) Item() iterator.Item {
	return iter.innerIter.Item()
}

func (iter *memIterator) Close() error {
	return iter.innerIter.Close()
}

type levelIterator struct {
	it    *iterator.Item
	iters []*Iterator
}

func (iter *memIterator) Seek(key []byte) {
}

func (lm *levelManager) NewIterator(options *iterator.Options) iterator.Iterator {
	return &levelIterator{}
}

func (iter *levelIterator) Next() {
}

func (iter *levelIterator) Valid() bool {
	return false
}

func (iter *levelIterator) Rewind() {

}

func (iter *levelIterator) Item() iterator.Item {
	return &Item{}
}

func (iter *levelIterator) Close() error {
	return nil
}

func (iter *levelIterator) Seek(key []byte) {
}
