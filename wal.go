package nimbusdb

import (
	"github.com/manosriram/nimbusdb/utils"
)

// KeyValueEntry is the raw and complete uncompressed data existing on the disk.
// KeyValueEntry is stored in Blocks in cache for faster reads.
type KeyValueEntry struct {
	deleted     byte
	blockNumber int64
	offset      int64
	size        int64 // Equals StaticChunkSize + keysize + valuesize
	tstamp      int64
	ksz         int64
	vsz         int64
	k           []byte
	v           []byte
	fileID      string
}

// Block represents a single block of disk memory. Default size is 32KB.
// Each Segment is a collection of blocks; Each block is a collection of KeyValueEntries.
type Block struct {
	entries     []*KeyValueEntry
	blockNumber int64
	blockOffset int64
}

func NewKeyValueEntry(deleted byte, offset, ksz, vsz, size int64, k, v []byte) *KeyValueEntry {
	return &KeyValueEntry{
		deleted: deleted,
		ksz:     ksz,
		vsz:     vsz,
		size:    size,
		k:       k,
		v:       v,
	}
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

func (db *Db) writeKeyValueEntry(keyValueEntry *KeyValueEntry) error {
	f, err := db.getActiveDataFilePointer()
	if err != nil {
		return err
	}
	_, err = f.Write(keyValueEntry.ToByte())
	return err
}
