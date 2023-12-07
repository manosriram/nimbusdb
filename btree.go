package nimbusdb

import (
	"sync"

	"github.com/google/btree"
)

type BTree struct {
	tree *btree.BTree
	mu   sync.RWMutex
}

func (b *BTree) Get(key []byte) *KeyDirValue {
	b.mu.RLock()
	defer b.mu.RUnlock()
	i := b.tree.Get(&item{key: key})
	if i != nil {
		return &i.(*item).v
	}
	return nil
}

func (b *BTree) Set(key []byte, value KeyDirValue) *KeyDirValue {
	b.mu.Lock()
	defer b.mu.Unlock()
	i := b.tree.ReplaceOrInsert(&item{key: key, v: value})
	if i != nil {
		return &i.(*item).v
	}
	return nil
}

func (b *BTree) Delete(key []byte) *KeyValuePair {
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
