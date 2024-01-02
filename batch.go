package nimbusdb

import (
	"bytes"
	"sync"

	"github.com/manosriram/nimbusdb/utils"
)

type Batch struct {
	db         *Db
	batchlock  sync.Mutex
	mu         sync.RWMutex
	writeQueue []*KeyValuePair
}

func (db *Db) NewBatch() *Batch {
	b := &Batch{
		db: db,
	}
	b.batchlock.Lock()
	return b
}

func (b *Batch) Close() error {
	if len(b.writeQueue) > 0 { // flush all pending queue writes to disk
		b.Commit()
	}
	b.batchlock.Unlock()
	return nil
}

func (b *Batch) Get(k []byte) ([]byte, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	index := -1
	var record *KeyValuePair
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			record = b.writeQueue[i]
			index = i
			break
		}
	}

	if index != -1 { // key found in write queue
		return utils.Encode(record.Value), nil
	}

	v, err := b.db.getKeyDir(k) // else, search datafiles
	if err != nil {
		return nil, err
	}
	return v.v, nil
}

func (b *Batch) Exists(k []byte) bool {
	b.mu.RLock()
	defer b.mu.RUnlock()
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			index = i
			break
		}
	}

	if index != -1 { // key found in write queue
		return true
	}

	v, err := b.db.getKeyDir(k) // else, search datafiles
	if err != nil {
		return false
	}
	return bytes.Equal(v.k, k)
}

func (b *Batch) Set(kv *KeyValuePair) error {
	k := kv.Key

	b.mu.Lock()
	defer b.mu.Unlock()
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			index = i
			break
		}
	}
	if index != -1 {
		b.writeQueue[index] = kv
	} else {
		b.writeQueue = append(b.writeQueue, kv)
	}
	return nil
}

// func (b *Batch) SetWithTTL(k []byte, v []byte, ttl time.Duration) error {
// b.mu.Lock()
// defer b.mu.Unlock()
// index := -1
// for i := range b.writeQueue {
// if bytes.Equal(k, b.writeQueue[i].Key) {
// index = i
// break
// }
// }
// kv := &KeyValuePair{
// Key:   k,
// Value: v,
// Ttl:   ttl,
// }

// if index != -1 {
// b.writeQueue[index] = kv
// } else {
// b.writeQueue = append(b.writeQueue, kv)
// }
// return nil
// }

func (b *Batch) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := range b.writeQueue {
		_, err := b.db.Set(b.writeQueue[i])
		if err != nil {
			return err
		}
	}
	b.writeQueue = nil
	return nil
}

func (b *Batch) Rollback() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.writeQueue = nil
	return nil
}
