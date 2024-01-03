package nimbusdb

import (
	"bytes"
	"sync"
	"time"

	"github.com/manosriram/nimbusdb/utils"
)

type Batch struct {
	db         *Db
	closed     bool
	batchlock  sync.Mutex
	mu         sync.RWMutex
	writeQueue []*KeyValuePair
}

func (db *Db) NewBatch() *Batch {
	b := &Batch{
		db:     db,
		closed: false,
	}
	b.batchlock.Lock()
	return b
}

func (b *Batch) Close() error {
	if b.db.closed {
		return ERROR_DB_CLOSED
	}
	if len(b.writeQueue) > 0 { // flush all pending queue writes to disk
		err := b.Commit()
		if err != nil {
			return err
		}
	}
	b.closed = true
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

func (b *Batch) Set(k []byte, v []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			index = i
			break
		}
	}
	kv := &KeyValuePair{
		Key:   k,
		Value: v,
	}
	if index != -1 {
		b.writeQueue[index] = kv
	} else {
		b.writeQueue = append(b.writeQueue, kv)
	}
	return v, nil
}

func (b *Batch) SetWithTTL(k []byte, v []byte, ttl time.Duration) (interface{}, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			index = i
			break
		}
	}
	kv := &KeyValuePair{
		Key:   k,
		Value: v,
		Ttl:   ttl,
	}
	if index != -1 {
		b.writeQueue[index] = kv
	} else {
		b.writeQueue = append(b.writeQueue, kv)
	}
	return v, nil
}

func (b *Batch) Delete(k []byte) error {
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
		b.writeQueue[index] = b.writeQueue[len(b.writeQueue)-1]
		b.writeQueue[len(b.writeQueue)-1] = nil
		b.writeQueue = b.writeQueue[:len(b.writeQueue)-1]
	} else {
		err := b.db.Delete(k)
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

func (b *Batch) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	var err error
	for i := range b.writeQueue {
		if b.writeQueue[i].Ttl == 0 {
			_, err = b.db.Set(b.writeQueue[i].Key, utils.Encode(b.writeQueue[i].Value))
		} else {
			_, err = b.db.SetWithTTL(b.writeQueue[i].Key, utils.Encode(b.writeQueue[i].Value), b.writeQueue[i].Ttl)
		}
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
