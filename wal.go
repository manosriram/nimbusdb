package nimbusdb

import (
	"time"

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

func (s *KeyValueEntry) setTTL(kv *KeyValuePair) {
	if kv.Ttl > 0 {
		s.tstamp = int64(time.Now().Add(kv.Ttl).UnixNano())
	} else {
		s.tstamp = int64(time.Now().Add(KEY_EXPIRES_IN_DEFAULT).UnixNano())
	}
}

func (db *Db) writeKeyValueEntry(keyValueEntry *KeyValueEntry) error {
	f, err := db.getActiveDataFilePointer()
	if err != nil {
		return err
	}
	_, err = f.Write(keyValueEntry.ToByte())
	return err
}

// Gets the KeyValueEntry from given offset using data slice
// Used to get the entire KeyValueEntry from file data
func getKeyValueEntryFromOffsetViaData(offset int64, data []byte) (*KeyValueEntry, error) {
	defer utils.Recover()

	if int(offset+StaticChunkSize) > len(data) {
		return nil, ERROR_OFFSET_EXCEEDED_FILE_SIZE
	}

	deleted := data[offset]

	tstamp := data[offset+DeleteFlagOffset : offset+TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)

	hasTimestampExpired := utils.HasTimestampExpired(tstamp64Bit)
	if hasTimestampExpired {
		return nil, ERROR_KEY_NOT_FOUND
	}

	// get key size
	ksz := data[offset+TstampOffset : offset+KeySizeOffset]
	intKsz := utils.ByteToInt64(ksz)

	// get value size
	vsz := data[offset+KeySizeOffset : offset+ValueSizeOffset]
	intVsz := utils.ByteToInt64(vsz)

	if int(offset+ValueSizeOffset+intKsz) > len(data) {
		return nil, ERROR_OFFSET_EXCEEDED_FILE_SIZE
	}
	// get key
	k := data[offset+ValueSizeOffset : offset+ValueSizeOffset+intKsz]

	if int(offset+ValueSizeOffset+intKsz+intVsz) > len(data) {
		return nil, ERROR_OFFSET_EXCEEDED_FILE_SIZE
	}
	// get value
	v := data[offset+ValueSizeOffset+intKsz : offset+ValueSizeOffset+intKsz+intVsz]

	// make keyValueEntry
	x := &KeyValueEntry{
		deleted: deleted,
		tstamp:  int64(tstamp64Bit),
		ksz:     int64(intKsz),
		vsz:     int64(intVsz),
		k:       k,
		v:       v,
		offset:  offset,
		size:    StaticChunkSize + intKsz + intVsz,
	}
	return x, nil
}

// Gets KeyValueEntry using offset and adds it to cacheBlock
// Used to add block's entries to cacheBlock.entries
func appendEntriesToBlock(data []byte, block *BlockOffsetPair, cacheBlock *Block) error {
	offset := block.startOffset
	for offset < block.endOffset {
		pair, err := getKeyValueEntryFromOffsetViaData(offset, data)
		if err != nil {
			return err
		}

		if pair != nil {
			offset += pair.size
			cacheBlock.entries = append(cacheBlock.entries, pair)
			continue
		}
		break
	}
	return nil
}
