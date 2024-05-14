package nimbusdb

import (
	"hash/crc32"
	"time"

	"github.com/manosriram/nimbusdb/utils"
)

var (
	crcTable = crc32.MakeTable(crc32.IEEE)
)

// KeyValueEntry is the raw and complete uncompressed data existing on the disk.
// KeyValueEntry is stored in Blocks in cache for faster reads.
type KeyValueEntry struct {
	crc         uint32
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

func (kv *KeyValueEntry) StaticChunkSize() int64 {
	return StaticChunkSize + kv.ksz + kv.vsz
}

func (kv *KeyValueEntry) Key() []byte {
	return kv.k
}

func (kv *KeyValueEntry) Value() []byte {
	return kv.v
}

func (kv *KeyValueEntry) PayloadToByte() []byte {
	keyValueEntryInBytes := make([]byte, 0, kv.StaticChunkSize())

	buf := make([]byte, 0)
	buf = append(buf, utils.Int64ToByte(kv.tstamp)...)
	buf = append(buf, utils.Int64ToByte(kv.ksz)...)
	buf = append(buf, utils.Int64ToByte(kv.vsz)...)

	keyValueEntryInBytes = append(keyValueEntryInBytes, buf...)
	keyValueEntryInBytes = append(keyValueEntryInBytes, kv.k...)
	keyValueEntryInBytes = append(keyValueEntryInBytes, kv.v...)

	return keyValueEntryInBytes
}

func (kv *KeyValueEntry) ToByte() []byte {
	keyValueEntryInBytes := make([]byte, 0, kv.StaticChunkSize())

	buf := make([]byte, 0)
	buf = append(buf, utils.UInt32ToByte(kv.crc)...)
	buf = append(buf, kv.deleted)
	buf = append(buf, utils.Int64ToByte(kv.tstamp)...)
	buf = append(buf, utils.Int64ToByte(kv.ksz)...)
	buf = append(buf, utils.Int64ToByte(kv.vsz)...)

	keyValueEntryInBytes = append(keyValueEntryInBytes, buf...)
	keyValueEntryInBytes = append(keyValueEntryInBytes, kv.k...)
	keyValueEntryInBytes = append(keyValueEntryInBytes, kv.v...)

	return keyValueEntryInBytes
}

func (kv *KeyValueEntry) setTTLViaTimestamp(tstamp int64) {
	kv.tstamp = tstamp
}

func (kv *KeyValueEntry) setTTLViaDuration(tstamp time.Duration) {
	kv.tstamp = int64(time.Now().Add(tstamp).UnixNano())
}

func (kv *KeyValueEntry) setCRC(crc uint32) {
	kv.crc = crc
}

func (kv *KeyValueEntry) calculateCRC() uint32 {
	data := kv.PayloadToByte()
	hash := crc32.Checksum(data, crcTable)
	return hash
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

	crc := data[offset : offset+CrcOffset]
	intCrc := utils.ByteToUInt32(crc)

	deleted := data[offset+CrcOffset]

	tstamp := data[offset+DeleteFlagOffset : offset+TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)

	hasTimestampExpired := utils.HasTimestampExpired(tstamp64Bit)

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

	keyValueEntryFromOffset := &KeyValueEntry{
		deleted: deleted,
		offset:  offset,
		ksz:     int64(len(k)),
		vsz:     int64(len(utils.Encode(v))),
		size:    int64(StaticChunkSize + intKsz + intVsz),
		k:       k,
		v:       utils.Encode(v),
	}
	keyValueEntryFromOffset.setTTLViaTimestamp(tstamp64Bit)

	if intCrc != keyValueEntryFromOffset.calculateCRC() {
		return nil, ERROR_CRC_DOES_NOT_MATCH
	}

	if hasTimestampExpired {
		return keyValueEntryFromOffset, ERROR_KEY_NOT_FOUND
	}
	return keyValueEntryFromOffset, nil
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
