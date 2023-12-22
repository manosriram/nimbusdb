package nimbusdb

import (
	"os"
	"time"

	"github.com/manosriram/nimbusdb/utils"
)

type KeyValueEntry struct {
	deleted     byte
	blockNumber int64
	fileID      string
	offset      int64
	size        int64 // Equals StaticChunkSize + keysize + valuesize
	tstamp      int64
	ksz         int64
	vsz         int64
	k           []byte
	v           []byte
}

type Block struct {
	entries     []*KeyValueEntry
	blockNumber int64
	blockOffset int64
}

func (s *KeyValueEntry) StaticChunkSize() int {
	return StaticChunkSize + len(s.k) + len(s.v)
}

func (s *KeyValueEntry) Key() []byte {
	return s.k
}

func (s *KeyValueEntry) Value() []byte {
	return s.v
}

func (s *KeyValueEntry) ToByte() []byte {
	keyValueEntryInBytes := make([]byte, 0, s.StaticChunkSize())

	buf := make([]byte, 0)
	buf = append(buf, s.deleted)
	buf = append(buf, utils.Int64ToByte(s.tstamp)...)
	buf = append(buf, utils.Int64ToByte(s.ksz)...)
	buf = append(buf, utils.Int64ToByte(s.vsz)...)

	keyValueEntryInBytes = append(keyValueEntryInBytes, buf...)
	keyValueEntryInBytes = append(keyValueEntryInBytes, s.k...)
	keyValueEntryInBytes = append(keyValueEntryInBytes, s.v...)

	return keyValueEntryInBytes
}

func (db *Db) WriteKeyValueEntry(keyValueEntry *KeyValueEntry) error {
	f, err := db.getActiveDataFilePointer()
	if err != nil {
		return err
	}
	_, err = f.Write(keyValueEntry.ToByte())
	return err
}

func (db *Db) ExpireKey(offset int64) error {
	f, err := os.OpenFile(db.activeDataFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	expireTstamp := time.Now().Add(-1 * time.Hour).UnixNano()
	_, err = f.WriteAt(utils.Int64ToByte(expireTstamp), int64(offset))
	if err != nil {
		return err
	}

	return nil
}
