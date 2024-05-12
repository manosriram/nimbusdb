package nimbusdb

import (
	"bytes"
	"errors"
	"time"

	"github.com/manosriram/nimbusdb/utils"
	"github.com/segmentio/ksuid"
)

var (
	ERROR_BATCH_CLOSED              = errors.New("batch is closed")
	ERROR_CANNOT_CLOSE_CLOSED_BATCH = errors.New("cannot close closed batch")
)

func (db *Db) NewBatch() (*Batch, error) {
	b := &Batch{
		db:     db,
		closed: false,
		id:     ksuid.New(),
	}
	b.batchlock.Lock()
	return b, nil
}

func (b *Batch) Close() error {
	if b.db.closed {
		return ERROR_DB_CLOSED
	}
	if b.closed {
		return ERROR_CANNOT_CLOSE_CLOSED_BATCH
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
	if b.closed {
		return nil, ERROR_BATCH_CLOSED
	}

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

	v, err := b.db.getKeyDirUsingKey(k) // else, search datafiles
	if err != nil {
		return nil, err
	}
	return v.value, nil
}

func (b *Batch) Exists(k []byte) (bool, error) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.closed {
		return false, ERROR_BATCH_CLOSED
	}
	index := -1
	for i := range b.writeQueue {
		if bytes.Equal(k, b.writeQueue[i].Key) {
			index = i
			break
		}
	}

	if index != -1 { // key found in write queue
		return true, nil
	}

	v, err := b.db.getKeyDirUsingKey(k) // else, search datafiles
	if err != nil {
		return false, err
	}
	return bytes.Equal(v.key, k), nil
}

func (b *Batch) Set(k []byte, v []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ERROR_BATCH_CLOSED
	}
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

func (b *Batch) SetWithTTL(k []byte, v []byte, ttl time.Duration) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ERROR_BATCH_CLOSED
	}
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

func (b *Batch) Delete(k []byte) ([]byte, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return nil, ERROR_BATCH_CLOSED
	}

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
		_, err := b.db.Delete(k)
		if err != nil {
			return nil, err
		}
		return k, nil
	}
	return k, nil
}

func (b *Batch) Commit() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ERROR_BATCH_CLOSED
	}

	for i := range b.writeQueue {
		k := b.writeQueue[i].Key
		v := utils.Encode(b.writeQueue[i].Value)
		var existingValueForKey []byte
		existingValueEntryForKey, err := b.db.Get(k)
		if err != nil {
			existingValueForKey = nil
		} else {
			existingValueForKey = existingValueEntryForKey
		}

		if b.writeQueue[i].Ttl == 0 {
			_, err = b.db.Set(k, v)
		} else {
			_, err = b.db.SetWithTTL(k, v, b.writeQueue[i].Ttl)
		}
		if err != nil {
			return err
		}

		if b.db.opts.ShouldWatch {
			if existingValueForKey == nil {
				b.db.SendWatchEvent(NewCreateWatcherEvent(k, existingValueForKey, v, &b.id))
			} else {
				b.db.SendWatchEvent(NewUpdateWatcherEvent(k, existingValueForKey, v, &b.id))
			}
		}
	}
	b.writeQueue = nil
	return nil
}

func (b *Batch) Rollback() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return ERROR_BATCH_CLOSED
	}
	b.writeQueue = nil
	return nil
}
