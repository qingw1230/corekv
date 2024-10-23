package lsm

import (
	"bytes"
	"fmt"
	"sort"

	"github.com/qingw1230/corekv/utils"
)

type Iterator struct {
	item  utils.Item
	iters []utils.Iterator
}

type Item struct {
	e *utils.Entry
}

func (it *Item) Entry() *utils.Entry {
	return it.e
}

func (lsm *LSM) NewIterator(opt *utils.Options) []utils.Iterator {
	iter := &Iterator{
		iters: make([]utils.Iterator, 0),
	}
	// 为跳表和不可变内存表创建迭代器
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))
	for _, t := range lsm.immutables {
		iter.iters = append(iter.iters, t.NewIterator(opt))
	}
	// 为 sst 文件创建迭代器
	iter.iters = append(iter.iters, lsm.lm.iterators()...)
	return iter.iters
}

func (it *Iterator) Next() {
	it.iters[0].Next()
}

func (it *Iterator) Valid() bool {
	return it.iters[0].Valid()
}

func (it *Iterator) Seek(key []byte) {
}

func (it *Iterator) Item() utils.Item {
	return it.iters[0].Item()
}

func (it *Iterator) Rewind() {
	it.iters[0].Rewind()
}

func (it *Iterator) Close() error {
	return nil
}

// memIterator 内存表迭代器
type memIterator struct {
	slIter utils.Iterator
}

func (m *memTable) NewIterator(opt *utils.Options) utils.Iterator {
	return &memIterator{
		slIter: m.sl.NewIterator(),
	}
}

func (it *memIterator) Next() {
	it.slIter.Next()
}

func (it *memIterator) Valid() bool {
	return it.slIter.Valid()
}

func (it *memIterator) Seek(key []byte) {
}

func (it *memIterator) Item() utils.Item {
	return it.slIter.Item()
}

func (it *memIterator) Rewind() {
	it.slIter.Rewind()
}

func (it *memIterator) Close() error {
	return it.slIter.Close()
}

// ConcatIterator 用于遍历一组 sst 文件
type ConcatIterator struct {
	opt    *utils.Options
	tables []*table         // 待遍历的 sst 文件
	iters  []utils.Iterator // sst 文件对应的迭代器
	cur    utils.Iterator   // 当前使用的迭代器
	idx    int              // 活跃迭代器下标
}

func NewConcatIterator(tables []*table, opt *utils.Options) *ConcatIterator {
	return &ConcatIterator{
		opt:    opt,
		tables: tables,
		iters:  make([]utils.Iterator, len(tables)),
		idx:    -1,
	}
}

func (ci *ConcatIterator) Next() {
	ci.cur.Next()
	if ci.cur.Valid() {
		return
	}
	// 当前 sst 文件遍历完了，遍历下一个
	for {
		if ci.opt.IsAsc {
			ci.setIdx(ci.idx + 1)
		} else {
			ci.setIdx(ci.idx - 1)
		}
		if ci.cur == nil {
			return
		}
		ci.cur.Rewind()
		if ci.cur.Valid() {
			break
		}
	}
}

func (ci *ConcatIterator) setIdx(idx int) {
	ci.idx = idx
	if idx < 0 || idx >= len(ci.iters) {
		ci.cur = nil
		return
	}
	if ci.iters[idx] == nil {
		ci.iters[idx] = ci.tables[idx].NewIterator(ci.opt)
	}
	ci.cur = ci.iters[idx]
}

func (ci *ConcatIterator) Valid() bool {
	return ci.cur != nil && ci.cur.Valid()
}

func (ci *ConcatIterator) Seek(key []byte) {
	var idx int
	// 先确定 key 在哪个 sst 文件
	if ci.opt.IsAsc {
		idx = sort.Search(len(ci.tables), func(i int) bool {
			return utils.CompareKeys(ci.tables[i].sst.MaxKey(), key) >= 0
		})
	} else {
		n := len(ci.tables)
		idx = n - 1 - sort.Search(n, func(i int) bool {
			return utils.CompareKeys(ci.tables[n-1-i].sst.MinKey(), key) <= 0
		})
	}
	if idx >= len(ci.tables) || idx < 0 {
		ci.setIdx(-1)
		return
	}

	ci.setIdx(idx)
	ci.cur.Seek(key)
}

func (ci *ConcatIterator) Item() utils.Item {
	return ci.cur.Item()
}

func (ci *ConcatIterator) Rewind() {
	if len(ci.iters) == 0 {
		return
	}
	if ci.opt.IsAsc {
		ci.setIdx(0)
	} else {
		ci.setIdx(len(ci.iters) - 1)
	}
	ci.cur.Rewind()
}

func (ci *ConcatIterator) Close() error {
	for _, it := range ci.iters {
		if it == nil {
			continue
		}
		if err := it.Close(); err != nil {
			return fmt.Errorf("ConcatIterator:%+v", err)
		}
	}
	return nil
}

// MergeIterator 多路合并迭代器
type MergeIterator struct {
	left  node  // 左迭代器
	right node  // 右迭代器
	small *node // 正向遍历时，指向 left、right 较小的呢个

	curKey  []byte
	reverse bool
}

type node struct {
	valid  bool
	entry  *utils.Entry
	iter   utils.Iterator
	merge  *MergeIterator
	concat *ConcatIterator
}

func NewMergeIterator(iters []utils.Iterator, reverse bool) utils.Iterator {
	switch len(iters) {
	case 0:
		return &Iterator{}
	case 1:
		return iters[0]
	case 2:
		mi := &MergeIterator{
			reverse: reverse,
		}
		mi.left.setIterator(iters[0])
		mi.right.setIterator(iters[1])
		// 此时 small 指向是随机的
		mi.small = &mi.left
		return mi
	}

	mid := len(iters) / 2
	return NewMergeIterator(
		[]utils.Iterator{
			NewMergeIterator(iters[:mid], reverse),
			NewMergeIterator(iters[mid:], reverse),
		}, reverse)
}

// setCurrent 将 mi.curKey 更新为 small 所指向的 key
func (mi *MergeIterator) setCurrent() {
	utils.CondPanic(mi.small.entry == nil && mi.small.valid, fmt.Errorf("mi.small.entry is nil"))
	if mi.small.valid {
		mi.curKey = append(mi.curKey[:0], mi.small.entry.Key...)
	}
}

func (mi *MergeIterator) Next() {
	for mi.Valid() {
		if !bytes.Equal(mi.small.entry.Key, mi.curKey) {
			break
		}
		mi.small.next()
		mi.fix()
	}
	mi.setCurrent()
}

// bigger 获取 mi.small 的另一边
func (mi *MergeIterator) bigger() *node {
	if mi.small == &mi.left {
		return &mi.right
	}
	return &mi.left
}

// swapSmall 将 mi.small 指向另一边
func (mi *MergeIterator) swapSmall() {
	if mi.small == &mi.left {
		mi.small = &mi.right
	} else {
		mi.small = &mi.left
	}
}

// fix 修正 mi.small 的指向
func (mi *MergeIterator) fix() {
	if !mi.bigger().valid {
		return
	}
	if !mi.small.valid {
		mi.swapSmall()
		return
	}

	comp := utils.CompareKeys(mi.small.entry.Key, mi.bigger().entry.Key)
	switch {
	case comp == 0:
		// 如果 key 相同，则将右迭代器向前移动
		mi.right.next()
		if &mi.right == mi.small {
			mi.swapSmall()
		}
	case comp < 0:
		// small 当前指向的是更小的
		if mi.reverse {
			mi.swapSmall()
		}
	default:
		// small 当前指向的是更大的
		if !mi.reverse {
			mi.swapSmall()
		}
	}
}

func (mi *MergeIterator) Valid() bool {
	return mi.small.valid
}

func (mi *MergeIterator) Seek(key []byte) {
	mi.left.seek(key)
	mi.right.seek(key)
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) Item() utils.Item {
	return mi.small.iter.Item()
}

func (mi *MergeIterator) Rewind() {
	mi.left.rewind()
	mi.right.rewind()
	mi.fix()
	mi.setCurrent()
}

func (mi *MergeIterator) Close() error {
	err1 := mi.left.iter.Close()
	err2 := mi.right.iter.Close()
	if err1 != nil {
		return utils.WarpErr("MergeIterator", err1)
	}
	return utils.WarpErr("MergeIterator", err2)
}

func (n *node) setIterator(iter utils.Iterator) {
	n.iter = iter
	n.merge, _ = iter.(*MergeIterator)
	n.concat, _ = iter.(*ConcatIterator)
}

// setKey 根据 iter 设置 entry
func (n *node) setKey() {
	switch {
	case n.merge != nil:
		n.valid = n.merge.small.valid
		if n.valid {
			n.entry = n.merge.small.entry
		}
	case n.concat != nil:
		n.valid = n.concat.Valid()
		if n.valid {
			n.entry = n.concat.Item().Entry()
		}
	default:
		n.valid = n.iter.Valid()
		if n.valid {
			n.entry = n.iter.Item().Entry()
		}
	}
}

// next 遍历下一个 key
func (n *node) next() {
	switch {
	case n.merge != nil:
		n.merge.Next()
	case n.concat != nil:
		n.concat.Next()
	default:
		n.iter.Next()
	}
	n.setKey()
}

func (n *node) seek(key []byte) {
	n.iter.Seek(key)
	n.setKey()
}

func (n *node) rewind() {
	n.iter.Rewind()
	n.setKey()
}
