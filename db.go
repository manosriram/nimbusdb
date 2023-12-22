package nimbusdb

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/google/btree"
	"github.com/hashicorp/golang-lru/v2/expirable"
	utils "github.com/manosriram/nimbusdb/utils"
)

const (
	_        = iota
	KB int64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

const (
	ActiveKeyValueEntryDatafileSuffix   = ".dfile"
	KeyValueEntryHintfileSuffix         = ".hfile"
	InactiveKeyValueEntryDataFileSuffix = ".idfile"
	TempDataFilePattern                 = "*.dfile"
	TempInactiveDataFilePattern         = "*.idfile"
	DefaultDataDir                      = "nimbusdb"

	DatafileThreshold = 1 * MB
	BlockSize         = 32 * KB
)

const (
	DeleteFlagOffset int64 = 1
	TstampOffset           = 11
	KeySizeOffset          = 21
	ValueSizeOffset        = 31

	StaticChunkSize = 1 + 10 + 10 + 10
	BTreeDegree     = 10
)
const (
	TotalStaticChunkSize int64 = TstampOffset + KeySizeOffset + ValueSizeOffset + DeleteFlagOffset + StaticChunkSize
)

const (
	KEY_EXPIRES_IN_DEFAULT = 24 * time.Hour

	KEY_NOT_FOUND             = "key expired or does not exist"
	NO_ACTIVE_FILE_OPENED     = "no file opened for writing"
	OFFSET_EXCEEDED_FILE_SIZE = "offset exceeded file size"
	CANNOT_READ_FILE          = "error reading file"

	DELETED_FLAG_BYTE_VALUE  = byte(0x31)
	DELETED_FLAG_SET_VALUE   = byte(0x01)
	DELETED_FLAG_UNSET_VALUE = byte(0x00)

	LRU_SIZE = 50
	LRU_TTL  = 24 * time.Hour
)

var (
	KEY_VALUE_SIZE_EXCEEDED = fmt.Sprintf("exceeded limit of %d bytes", BlockSize)
)

type Options struct {
	IsMerge       bool
	MergeFilePath string
	Path          string
}

type KeyValuePair struct {
	Key   []byte
	Value interface{}
	Ttl   time.Duration
}

type KeyDirValue struct {
	offset      int64
	blockNumber int64
	size        int64
	path        string
	tstamp      int64
}

func NewKeyDirValue(offset, size, tstamp int64, path string) *KeyDirValue {
	return &KeyDirValue{
		offset: offset,
		size:   size,
		tstamp: tstamp,
		path:   path,
	}
}

type Db struct {
	dirPath                  string
	dataFilePath             string
	activeDataFile           string
	activeDataFilePointer    *os.File
	inActiveDataFilePointers *sync.Map
	keyDir                   *BTree
	opts                     *Options
	lastOffset               atomic.Int64
	mu                       sync.RWMutex
	segments                 map[string]*Segment
	lru                      *expirable.LRU[int64, *Block]
}

func NewDb(dirPath string) *Db {
	segments := make(map[string]*Segment)
	db := &Db{
		dirPath: dirPath,
		keyDir: &BTree{
			tree: btree.New(BTreeDegree),
		},
		lru:                      expirable.NewLRU[int64, *Block](LRU_SIZE, nil, LRU_TTL),
		segments:                 segments,
		inActiveDataFilePointers: &sync.Map{},
	}

	return db
}

func (db *Db) setLastOffset(v int64) {
	db.lastOffset.Store(v)
}

func (db *Db) getLastOffset() int64 {
	return db.lastOffset.Load()
}

func (db *Db) getActiveDataFilePointer() (*os.File, error) {
	if db.activeDataFilePointer == nil {
		return nil, errors.New(NO_ACTIVE_FILE_OPENED)
	}
	return db.activeDataFilePointer, nil
}

func (db *Db) closeActiveDataFilePointer() error {
	if db.activeDataFilePointer != nil {
		return db.activeDataFilePointer.Close()
	}
	return nil
}

func (db *Db) setActiveDataFile(activeDataFile string) error {
	err := db.closeActiveDataFilePointer()
	if err != nil {
		return err
	}

	f, err := os.OpenFile(activeDataFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	db.activeDataFile = activeDataFile
	db.activeDataFilePointer = f
	return nil
}

func (db *Db) setKeyDir(key []byte, kdValue KeyDirValue) (interface{}, error) {
	if len(key) == 0 || kdValue.offset < 0 {
		return nil, nil
	}

	if kdValue.size > BlockSize {
		return nil, errors.New(KEY_VALUE_SIZE_EXCEEDED)
	}

	segment, ok := db.segments[kdValue.path]
	if !ok {
		newSegment := &Segment{
			blocks: map[int64]*BlockOffsetPair{
				0: {startOffset: kdValue.offset,
					endOffset: kdValue.offset + kdValue.size,
					filePath:  kdValue.path,
				},
			},
			path:               kdValue.path,
			currentBlockNumber: 0,
			currentBlockOffset: 0,
		}

		fp, err := db.getSegmentFilePointerFromPath(kdValue.path)
		if err != nil {
			return nil, err
		}
		newSegment.fp = fp
		newSegment.closed = false

		db.setSegment(kdValue.path, newSegment)
	} else {
		segmentBlock, ok := db.getSegmentBlock(kdValue.path, segment.currentBlockNumber)
		if !ok {
			return nil, errors.New(CANNOT_READ_FILE)
		}
		segmentBlock.endOffset = kdValue.offset + kdValue.size
		db.setSegmentBlock(kdValue.path, segment.currentBlockNumber, segmentBlock)
		if segment.currentBlockOffset+kdValue.size <= BlockSize {
			kdValue.blockNumber = segment.currentBlockNumber
			db.setSegmentBlockOffset(kdValue.path, db.getSegmentBlockOffset(kdValue.path)+kdValue.size)
		} else {
			segment.currentBlockNumber += 1
			segment.blocks[segment.currentBlockNumber] = &BlockOffsetPair{
				startOffset: kdValue.offset,
				endOffset:   kdValue.offset + kdValue.size,
				filePath:    kdValue.path,
			}
			kdValue.blockNumber = segment.currentBlockNumber
			db.setSegmentBlockOffset(kdValue.path, kdValue.size)
		}
		db.setSegment(kdValue.path, segment)
	}

	db.keyDir.Set(key, kdValue)
	db.lastOffset.Store(kdValue.offset + kdValue.size)
	db.lru.Remove(db.getSegmentBlockNumber(kdValue.path))

	return kdValue, nil
}

func (db *Db) getKeyDir(key []byte) (*KeyValueEntry, error) {
	var cacheBlock = new(Block)

	kv := db.keyDir.Get(key)
	if kv == nil || kv.blockNumber == -1 {
		return nil, errors.New(KEY_NOT_FOUND)
	}

	cachedBlock, ok := db.lru.Get(kv.blockNumber)
	if ok {
		cacheBlock = cachedBlock

		// TODO: optimize this code block
		for _, entry := range cacheBlock.entries {
			if bytes.Compare(key, entry.k) == 0 {
				return entry, nil
			}
		}
	}

	var v *KeyValueEntry
	segment := db.segments[kv.path]
	block, ok := db.getSegmentBlock(kv.path, kv.blockNumber)
	if !ok {
		return nil, errors.New(CANNOT_READ_FILE)
	}
	data, err := db.getKeyValueEntryFromOffsetViaFilePath(segment.path)
	if err != nil {
		return nil, err
	}

	offset := block.startOffset
	for offset < block.endOffset {
		pair, err := getKeyValueEntryFromOffsetViaData(offset, data)
		if err != nil {
			return nil, err
		}

		if pair != nil {
			if pair.offset == kv.offset {
				v = pair
			}
			offset += pair.size
			cacheBlock.entries = append(cacheBlock.entries, pair)
			continue
		}
		break
	}

	if v != nil {
		tstampString, err := strconv.ParseInt(fmt.Sprint(v.tstamp), 10, 64)
		if err != nil {
			return nil, err
		}
		hasTimestampExpired := utils.HasTimestampExpired(tstampString)
		if hasTimestampExpired {
			db.keyDir.Delete(key)
			return nil, errors.New(KEY_NOT_FOUND)
		}
		db.lru.Add(kv.blockNumber, cacheBlock)
		return v, nil
	}

	return nil, errors.New(KEY_NOT_FOUND)
}

func (db *Db) getKeyValueEntryFromOffsetViaFilePath(keyDirPath string) ([]byte, error) {
	// TODO: improve file handling here
	var data []byte
	path := filepath.Join(db.dirPath, keyDirPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
}

func getKeyValueEntryFromOffsetViaData(offset int64, data []byte) (*KeyValueEntry, error) {
	defer utils.Recover()

	if int(offset+StaticChunkSize) > len(data) {
		return nil, errors.New(OFFSET_EXCEEDED_FILE_SIZE)
	}

	deleted := data[offset]

	tstamp := data[offset+DeleteFlagOffset : offset+TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)

	hasTimestampExpired := utils.HasTimestampExpired(tstamp64Bit)
	if hasTimestampExpired {
		return nil, errors.New(KEY_NOT_FOUND)
	}

	// get key size
	ksz := data[offset+TstampOffset : offset+KeySizeOffset]
	intKsz := utils.ByteToInt64(ksz)

	// get value size
	vsz := data[offset+KeySizeOffset : offset+ValueSizeOffset]
	intVsz := utils.ByteToInt64(vsz)

	if int(offset+ValueSizeOffset+intKsz) > len(data) {
		return nil, errors.New(OFFSET_EXCEEDED_FILE_SIZE)
	}
	// get key
	k := data[offset+ValueSizeOffset : offset+ValueSizeOffset+intKsz]

	if int(offset+ValueSizeOffset+intKsz+intVsz) > len(data) {
		return nil, errors.New(OFFSET_EXCEEDED_FILE_SIZE)
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

func (db *Db) seekOffsetFromDataFile(kdValue KeyDirValue) (*KeyValueEntry, error) {
	defer utils.Recover()

	f, err := db.getSegmentFilePointerFromPath(kdValue.path)
	if err != nil {
		return nil, err
	}

	data := make([]byte, kdValue.size)
	f.Seek(kdValue.offset, io.SeekCurrent)
	f.Read(data)

	deleted := data[0]
	if deleted == DELETED_FLAG_BYTE_VALUE {
		return nil, errors.New(KEY_NOT_FOUND)
	}

	tstamp := data[DeleteFlagOffset:TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)
	hasTimestampExpired := utils.HasTimestampExpired(tstamp64Bit)
	if hasTimestampExpired {
		return nil, errors.New(KEY_NOT_FOUND)
	}

	ksz := data[TstampOffset:KeySizeOffset]
	intKsz := utils.ByteToInt64(ksz)

	// get value size
	vsz := data[KeySizeOffset:ValueSizeOffset]
	intVsz := utils.ByteToInt64(vsz)

	// get key
	k := data[ValueSizeOffset : ValueSizeOffset+intKsz]

	// get value
	v := data[ValueSizeOffset+intKsz : ValueSizeOffset+intKsz+intVsz]

	return &KeyValueEntry{
		tstamp: int64(tstamp64Bit),
		ksz:    int64(intKsz),
		vsz:    int64(intVsz),
		k:      k,
		v:      v,
		offset: kdValue.offset,
		size:   kdValue.size,
	}, nil
}

func (db *Db) getActiveFileKeyValueEntries(filePath string) ([]*KeyValueEntry, error) {
	defer utils.Recover()

	data, err := utils.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	keyValueEntries := make([]*KeyValueEntry, 0)

	var offset int64 = 0
	for offset < int64(len(data)) {
		keyValueEntry, err := getKeyValueEntryFromOffsetViaData(offset, data)
		if err != nil {
			return nil, err
		}

		if keyValueEntry.deleted != DELETED_FLAG_BYTE_VALUE {
			keyValueEntry.fileID = strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
			hasTimestampExpired := utils.HasTimestampExpired(keyValueEntry.tstamp)
			if !hasTimestampExpired {
				split := strings.Split(filePath, "/")
				fileName := split[len(split)-1]
				kdValue := KeyDirValue{
					offset: keyValueEntry.offset,
					size:   keyValueEntry.size,
					path:   fileName,
					tstamp: keyValueEntry.tstamp,
				}
				_, err := db.setKeyDir(keyValueEntry.k, kdValue) // TODO: use Set here?
				if err != nil {
					return nil, err
				}
				keyValueEntries = append(keyValueEntries, keyValueEntry)
			}
		}

		if int(offset+StaticChunkSize) > len(data) {
			offset += keyValueEntry.size
			break
		}

		offset += keyValueEntry.size
	}
	return keyValueEntries, nil
}

func (db *Db) parseActiveKeyValueEntryFile(filePath string) error {
	data, err := utils.ReadFile(filePath) // TODO: read in blocks
	if err != nil {
		return err
	}

	var offset int64 = 0
	for offset < int64(len(data)) {
		keyValueEntry, err := getKeyValueEntryFromOffsetViaData(offset, data)
		if err != nil {
			return err
		}

		if keyValueEntry.deleted != DELETED_FLAG_BYTE_VALUE {
			keyValueEntry.fileID = strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
			hasTimestampExpired := utils.HasTimestampExpired(keyValueEntry.tstamp)
			if !hasTimestampExpired {
				fileName := utils.GetFilenameWithoutExtension(filePath)
				kdValue := KeyDirValue{
					offset: keyValueEntry.offset,
					size:   keyValueEntry.size,
					path:   fileName,
					tstamp: keyValueEntry.tstamp,
				}
				_, err := db.setKeyDir(keyValueEntry.k, kdValue) // TODO: use Set here?
				if err != nil {
					return err
				}
			}
		}

		if int(offset+StaticChunkSize) > len(data) {
			offset += keyValueEntry.size
			break
		}

		offset += keyValueEntry.size
	}
	return nil
}

func (db *Db) createInactiveDatafile(dirPath string) error {
	file, err := os.CreateTemp(dirPath, TempInactiveDataFilePattern)
	db.setActiveDataFile(file.Name())
	db.setLastOffset(0)
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) createActiveDatafile(dirPath string) error {
	defer utils.Recover()

	dir, err := os.ReadDir(dirPath)
	if err != nil {
		return err
	}
	for _, file := range dir {
		if file.IsDir() {
			continue
		}
		extension := path.Ext(file.Name())
		if extension == ActiveKeyValueEntryDatafileSuffix {
			inactiveName := fmt.Sprintf("%s.idfile", strings.Split(file.Name(), ".")[0])

			oldPath := filepath.Join(dirPath, file.Name())
			newPath := filepath.Join(dirPath, inactiveName)
			os.Rename(oldPath, newPath)

			fp, err := db.getSegmentFilePointerFromPath(inactiveName)
			if err != nil {
				return err
			}
			db.setSegment(inactiveName, db.getSegment(file.Name()))
			db.setSegmentPath(inactiveName, inactiveName)
			db.setSegmentFp(inactiveName, fp)
		}
	}

	file, err := os.CreateTemp(dirPath, TempDataFilePattern)
	db.setActiveDataFile(file.Name())
	db.setLastOffset(0)
	if err != nil {
		return err
	}
	return nil
}

// Closes the database. Closes the file pointer used to read/write the activeDataFile.
// Closes all file inactiveDataFile pointers and marks them as closed.
func (db *Db) Close() error {
	if db.activeDataFilePointer != nil {
		err := db.activeDataFilePointer.Close()
		return err
	}
	for _, segment := range db.segments {
		if !segment.closed {
			segment.fp.Close()
			segment.closed = true
		}
	}
	return nil
}

func Open(opts *Options) (*Db, error) {
	defer utils.Recover()

	dirPath := opts.Path
	if dirPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}

		dirPath = utils.JoinPaths(home, DefaultDataDir)
	}
	db := NewDb(dirPath)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return nil, err
	}

	dir, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Empty path, starting new
	if len(dir) == 0 {
		err = db.createActiveDatafile(dirPath)
		if err != nil {
			return nil, err
		}
	}

	filepath.WalkDir(dirPath, func(s string, file fs.DirEntry, e error) error {
		if file.IsDir() {
			return nil
		}

		filePath := utils.JoinPaths(dirPath, file.Name())
		if path.Ext(file.Name()) == ActiveKeyValueEntryDatafileSuffix {
			db.setActiveDataFile(filePath)
			db.parseActiveKeyValueEntryFile(db.activeDataFile)
		} else if path.Ext(file.Name()) == KeyValueEntryHintfileSuffix {
			// TODO
			return nil
		} else if path.Ext(file.Name()) == InactiveKeyValueEntryDataFileSuffix {
			db.parseActiveKeyValueEntryFile(filePath)
		}

		return nil
	})
	return db, nil
}

func (db *Db) All() []*KeyValuePair {
	return db.keyDir.List()
}

func (db *Db) limitDatafileToThreshold(newKeyValueEntry *KeyValueEntry, opts *Options) {
	var sz os.FileInfo
	var err error
	f, err := db.getActiveDataFilePointer()
	sz, err = f.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := sz.Size()

	if size+newKeyValueEntry.size > DatafileThreshold {
		if opts.IsMerge {
			db.createInactiveDatafile(db.dirPath)
			os.Remove(opts.MergeFilePath)
		} else {
			db.createActiveDatafile(db.dirPath)
			newKeyValueEntry.offset = 0
		}
	}
}

func (db *Db) deleteKey(key []byte) error {
	// TODO: move this to someplace better
	db.mu.Lock()
	defer db.mu.Unlock()

	v := db.keyDir.Get(key)
	if v == nil {
		return errors.New(KEY_NOT_FOUND)
	}

	f := db.segments[v.path]
	f.fp.WriteAt([]byte{DELETED_FLAG_SET_VALUE}, v.offset)
	db.lru.Remove(v.blockNumber)
	db.keyDir.Delete(key)

	return nil
}

// Gets a key-value pair.
// Returns the value if the key exists and error if any.
func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	v, _ := db.getKeyDir(key)
	if v == nil {
		return nil, errors.New(KEY_NOT_FOUND)
	}
	return v.v, nil
}

// Sets a key-value pair.
// Returns the value if set succeeds, else returns an error.
func (db *Db) Set(kv *KeyValuePair) (interface{}, error) {

	intKSz := int64(len(kv.Key))
	intVSz := int64(len(utils.Encode(kv.Value)))

	newKeyValueEntry := NewKeyValueEntry(
		DELETED_FLAG_UNSET_VALUE,
		db.getLastOffset(),
		int64(len(kv.Key)),
		int64(len(utils.Encode(kv.Value))),
		int64(StaticChunkSize+intKSz+intVSz),
		kv.Key,
		utils.Encode(kv.Value),
	)
	if kv.Ttl > 0 {
		newKeyValueEntry.tstamp = int64(time.Now().Add(kv.Ttl).UnixNano())
	} else {
		newKeyValueEntry.tstamp = int64(time.Now().Add(KEY_EXPIRES_IN_DEFAULT).UnixNano())
	}

	db.mu.Lock()
	defer db.mu.Unlock()
	db.limitDatafileToThreshold(newKeyValueEntry, &Options{})
	err := db.writeKeyValueEntry(newKeyValueEntry)
	if err != nil {
		return nil, err
	}

	kdValue := NewKeyDirValue(newKeyValueEntry.offset, newKeyValueEntry.size, newKeyValueEntry.tstamp, utils.GetFilenameWithoutExtension(db.activeDataFile))
	_, err = db.setKeyDir(kv.Key, *kdValue)
	if err != nil {
		return nil, err
	}

	return kv.Value, err
}

// Deletes a key-value pair.
// Returns error if any.
func (db *Db) Delete(key []byte) error {
	err := db.deleteKey(key)
	return err
}

func (db *Db) walk(s string, file fs.DirEntry, err error) error {
	if path.Ext(file.Name()) != InactiveKeyValueEntryDataFileSuffix {
		return nil
	}

	path := utils.JoinPaths(db.dirPath, file.Name())
	db.setActiveDataFile(path)
	db.setLastOffset(0)

	keyValueEntries, _ := db.getActiveFileKeyValueEntries(path)
	if len(keyValueEntries) == 0 {
		err = os.Remove(path)
		if err != nil {
			return err
		}
	}

	for _, keyValueEntry := range keyValueEntries {
		db.limitDatafileToThreshold(keyValueEntry, &Options{
			IsMerge:       true,
			MergeFilePath: path,
		})
		err := db.writeKeyValueEntry(keyValueEntry)
		if err != nil {
			return err
		}
	}
	return nil
}

// Syncs the database. Will remove all expired/deleted keys from disk.
// Since items are removed, disk usage will reduce.
func (db *Db) Sync() error {
	err := filepath.WalkDir(db.dirPath, db.walk)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}
