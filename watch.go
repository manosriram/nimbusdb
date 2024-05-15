package nimbusdb

import (
	"time"

	"github.com/segmentio/ksuid"
)

type EventType string

const (
	Create EventType = "CREATE"
	Update EventType = "UPDATE"
	Delete EventType = "DELETE"
)

type WatcherEvent struct {
	EventType      EventType
	Key            []byte
	OldValue       []byte
	NewValue       []byte
	EventTimestamp time.Time
	BatchId        *ksuid.KSUID
}

func (db *Db) NewWatch() (chan WatcherEvent, error) {
	if db.closed {
		return nil, ERROR_DB_CLOSED
	}
	return db.watcher, nil
}

func (db *Db) CloseWatch() error {
	if db.closed {
		return ERROR_DB_CLOSED
	}
	if db.watcher != nil {
		close(db.watcher)
	}
	return nil
}

func NewCreateWatcherEvent(key, oldValue, newValue []byte, batchId *ksuid.KSUID) WatcherEvent {
	w := WatcherEvent{
		EventType:      Create,
		Key:            key,
		NewValue:       newValue,
		EventTimestamp: time.Now(),
		BatchId:        batchId,
	}
	if oldValue != nil {
		w.OldValue = oldValue
	}
	return w
}

func NewUpdateWatcherEvent(key, oldValue, newValue []byte, batchId *ksuid.KSUID) WatcherEvent {
	w := WatcherEvent{
		EventType:      Update,
		Key:            key,
		NewValue:       newValue,
		EventTimestamp: time.Now(),
		BatchId:        batchId,
	}
	if oldValue != nil {
		w.OldValue = oldValue
	}
	return w
}

func NewDeleteWatcherEvent(key, oldValue, newValue []byte, batchId *ksuid.KSUID) WatcherEvent {
	w := WatcherEvent{
		EventType:      Delete,
		Key:            key,
		NewValue:       newValue,
		EventTimestamp: time.Now(),
		BatchId:        batchId,
	}
	if oldValue != nil {
		w.OldValue = oldValue
	}
	return w
}

func (db *Db) SendWatchEvent(w WatcherEvent) error {
	if db.closed {
		return ERROR_DB_CLOSED
	}
	db.watcher <- w
	return nil
}
