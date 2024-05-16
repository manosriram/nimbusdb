package nimbusdb

import (
	"bytes"
	"sync"

	"github.com/google/btree"
	"github.com/manosriram/nimbusdb/utils"
)

type BTree struct {
	tree *btree.BTree
	mu   sync.RWMutex
}

type item struct {
	key []byte
	v   KeyDirValue
}

var itemPool = sync.Pool{
	New: func() interface{} {
		return &item{}
	},
}

func (it item) Less(i btree.Item) bool {
	return bytes.Compare(it.key, i.(*item).key) < 0
}

func (b *BTree) Get(key []byte) *KeyDirValue {
	it := itemPool.Get().(*item)
	it.key = key
	defer itemPool.Put(it)

	i := b.tree.Get(it)
	if i == nil {
		return nil
	}
	return &i.(*item).v
}

func (b *BTree) Set(key []byte, value KeyDirValue) *KeyDirValue {
	i := b.tree.ReplaceOrInsert(&item{key: key, v: value})
	if i != nil {
		return &i.(*item).v
	}

	return nil
}

func (b *BTree) Delete(key []byte) *KeyValuePair {
	it := itemPool.Get().(*item)
	it.key = key
	defer itemPool.Put(it)

	i := b.tree.Delete(&item{key: key})
	if i != nil {
		x := i.(*item)
		return &KeyValuePair{
			Key:   x.key,
			Value: x.v,
		}
	}
	return nil
}

func (b *BTree) List() []*KeyValuePair {
	var pairs []*KeyValuePair
	b.tree.Ascend(func(it btree.Item) bool {
		pairs = append(pairs, &KeyValuePair{
			Key:   it.(*item).key,
			Value: it.(*item).v,
			Ttl:   utils.TimeUntilUnixNano(it.(*item).v.tstamp),
		})
		return true
	})
	return pairs
}
