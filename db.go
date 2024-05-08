package nimbusdb

import (
	"bytes"
	"errors"
	"fmt"
	"io/fs"
	"log"
	"os"
	"os/signal"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
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
	CrcSize         int64 = 5
	DeleteFlagSize        = 1
	TstampSize            = 10
	KeySizeSize           = 10
	ValueSizeSize         = 10
	StaticChunkSize       = CrcSize + DeleteFlagSize + TstampSize + KeySizeSize + ValueSizeSize

	CrcOffset        int64 = 5
	DeleteFlagOffset       = 6
	TstampOffset           = 16
	KeySizeOffset          = 26
	ValueSizeOffset        = 36

	BTreeDegree int = 10
)

const (
	TotalStaticChunkSize int64 = TstampOffset + KeySizeOffset + ValueSizeOffset + DeleteFlagOffset + CrcOffset + StaticChunkSize
)

var (
	ERROR_KEY_NOT_FOUND             = errors.New("key expired or does not exist")
	ERROR_NO_ACTIVE_FILE_OPENED     = errors.New("no file opened for writing")
	ERROR_OFFSET_EXCEEDED_FILE_SIZE = errors.New("offset exceeded file size")
	ERROR_CANNOT_READ_FILE          = errors.New("error reading file")
	ERROR_KEY_VALUE_SIZE_EXCEEDED   = errors.New(fmt.Sprintf("exceeded limit of %d bytes", BlockSize))
	ERROR_CRC_DOES_NOT_MATCH        = errors.New("crc does not match. corrupted datafile")
	ERROR_DB_CLOSED                 = errors.New("database is closed")
)

const (
	KEY_EXPIRES_IN_DEFAULT = 168 * time.Hour // 1 week

	DELETED_FLAG_BYTE_VALUE  = byte(0x31)
	DELETED_FLAG_SET_VALUE   = byte(0x01)
	DELETED_FLAG_UNSET_VALUE = byte(0x00)

	LRU_SIZE = 50
	LRU_TTL  = 24 * time.Hour

	EXIT_NOT_OK = 0
	EXIT_OK     = 1

	INITIAL_SEGMENT_OFFSET         = 0
	INITIAL_KEY_VALUE_ENTRY_OFFSET = 0
)

type Options struct {
	IsMerge        bool
	MergeFilePath  string
	Path           string
	ShouldWatch    bool
	WatchQueueSize int
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
	dirPath               string
	closed                bool
	dataFilePath          string
	activeDataFile        string
	activeDataFilePointer *os.File
	keyDir                *BTree
	opts                  *Options
	lastOffset            atomic.Int64
	mu                    sync.RWMutex
	segments              map[string]*Segment
	lru                   *expirable.LRU[int64, *Block]
	watcher               chan WatcherEvent
}

func NewDb(dirPath string, opts ...*Options) *Db {
	segments := make(map[string]*Segment, 0)
	db := &Db{
		dirPath: dirPath,
		closed:  false,
		keyDir: &BTree{
			tree: btree.New(BTreeDegree),
		},
		lru:      expirable.NewLRU[int64, *Block](LRU_SIZE, nil, LRU_TTL),
		segments: segments,
		opts: &Options{
			ShouldWatch: false,
		},
	}

	db.watcher = make(chan WatcherEvent, func() int {
		if len(opts) > 0 {
			return opts[0].WatchQueueSize
		}
		return 0
	}())
	if len(opts) > 0 {
		db.opts.ShouldWatch = opts[0].ShouldWatch
	}

	return db
}

func (db *Db) getBlockFromCache(blockNumber int64) (*Block, bool) {
	cachedBlock, ok := db.lru.Get(blockNumber)
	return cachedBlock, ok
}

func (db *Db) setBlockCache(blockNumber int64, block *Block) {
	db.lru.Add(blockNumber, block)
}

func (db *Db) removeBlockCache(blockNumber int64) {
	db.lru.Remove(blockNumber)
}

func (db *Db) setLastOffset(v int64) {
	db.lastOffset.Store(v)
}

func (db *Db) getLastOffset() int64 {
	return db.lastOffset.Load()
}

func (db *Db) getActiveDataFilePointer() (*os.File, error) {
	if db.activeDataFilePointer == nil {
		return nil, ERROR_NO_ACTIVE_FILE_OPENED
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
		return nil, ERROR_KEY_VALUE_SIZE_EXCEEDED
	}

	segment, ok := db.getSegment(kdValue.path)
	if !ok {
		newSegment := createNewSegment(&kdValue)
		fp, err := db.getSegmentFilePointerFromPath(kdValue.path)
		if err != nil {
			return nil, err
		}
		newSegment.fp = fp
		newSegment.closed = false

		db.setSegment(kdValue.path, newSegment)
	} else {
		segment, err := db.updateSegment(&kdValue, segment)
		if err != nil {
			return nil, err
		}
		db.removeBlockCache(segment.getBlockNumber())
	}
	db.keyDir.Set(key, kdValue)
	db.lastOffset.Store(kdValue.offset + kdValue.size)

	return kdValue, nil
}

func (db *Db) getKeyDir(key []byte) (*KeyValueEntry, error) {
	var cacheBlock = new(Block)

	kv := db.keyDir.Get(key)
	if kv == nil || kv.blockNumber == -1 {
		return nil, ERROR_KEY_NOT_FOUND
	}

	cachedBlock, ok := db.getBlockFromCache(kv.blockNumber)
	if ok {
		cacheBlock = cachedBlock
		// TODO: optimize this code block
		for _, entry := range cacheBlock.entries {
			if bytes.Compare(key, entry.k) == 0 {
				return entry, nil
			}
		}
	}

	segment, ok := db.getSegment(kv.path)
	if !ok {
		return nil, ERROR_CANNOT_READ_FILE
	}

	block, ok := db.getSegmentBlock(kv.path, kv.blockNumber)
	if !ok {
		return nil, ERROR_CANNOT_READ_FILE
	}
	data, err := db.getKeyValueEntryFromOffsetViaFilePath(segment.path)
	if err != nil {
		return nil, err
	}

	err = appendEntriesToBlock(data, block, cacheBlock)
	if err != nil {
		return nil, err
	}
	v, err := getKeyValueEntryFromOffsetViaData(kv.offset, data)
	if err != nil {
		return nil, err
	}

	if v != nil {
		tstampString, err := strconv.ParseInt(fmt.Sprint(v.tstamp), 10, 64)
		if err != nil {
			return nil, err
		}
		hasTimestampExpired := utils.HasTimestampExpired(tstampString)
		if hasTimestampExpired {
			db.keyDir.Delete(key)
			return nil, ERROR_KEY_NOT_FOUND
		}
		db.setBlockCache(kv.blockNumber, cacheBlock)
		return v, nil
	}

	return nil, ERROR_KEY_NOT_FOUND
}

func (db *Db) getKeyValueEntryFromOffsetViaFilePath(keyDirPath string) ([]byte, error) {
	var data []byte
	path := filepath.Join(db.dirPath, keyDirPath)
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	return data, nil
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
			keyValueEntry.fileID = utils.GetFilenameWithoutExtension(filePath)
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
			keyValueEntry.fileID = utils.GetFilenameWithoutExtension(filePath)
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
	db.setLastOffset(INITIAL_SEGMENT_OFFSET)
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
			segment, ok := db.getSegment(file.Name())
			if !ok {
				return ERROR_CANNOT_READ_FILE
			}
			db.setSegment(inactiveName, segment)
			segment.setPath(inactiveName)
			segment.setFp(fp)
		}
	}

	file, err := os.CreateTemp(dirPath, TempDataFilePattern)
	db.setActiveDataFile(file.Name())
	db.setLastOffset(INITIAL_SEGMENT_OFFSET)
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) handleInterrupt() {
	terminateSignal := make(chan os.Signal, 1)
	signal.Notify(terminateSignal, syscall.SIGINT, syscall.SIGKILL, syscall.SIGTERM)
	for {
		select {
		case s := <-terminateSignal:
			err := db.Close()
			if err != nil {
				log.Panicf("error closing DB: %s\n", err.Error())
			}
			log.Printf("closing DB via interrupt %v", s)
			os.Exit(EXIT_NOT_OK)
		}
	}
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
	db := NewDb(dirPath, opts)
	go db.handleInterrupt()

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

// Closes the database. Closes the file pointer used to read/write the activeDataFile.
// Closes all file inactiveDataFile pointers and marks them as closed.
func (db *Db) Close() error {
	if db.activeDataFilePointer != nil {
		err := db.activeDataFilePointer.Close()
		return err
	}
	for _, segment := range db.segments {
		err := segment.closeFp()
		if err != nil {
			return err
		}
	}
	db.closed = true
	close(db.watcher)
	return nil
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
			newKeyValueEntry.offset = INITIAL_KEY_VALUE_ENTRY_OFFSET
		}
	}
}

func (db *Db) deleteKey(key []byte) error {
	v := db.keyDir.Get(key)
	if v == nil {
		return ERROR_KEY_NOT_FOUND
	}

	segment, ok := db.getSegment(v.path)
	if !ok {
		return ERROR_CANNOT_READ_FILE
	}
	fp := segment.getFp()
	fp.WriteAt([]byte{DELETED_FLAG_SET_VALUE}, v.offset)
	db.removeBlockCache(v.blockNumber)
	db.keyDir.Delete(key)

	return nil
}

// Gets a key-value pair.
// Returns the value if the key exists and error if any.
func (db *Db) Get(key []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	v, err := db.getKeyDir(key)
	if v == nil {
		return nil, err
	}
	return v.v, nil
}

// Sets a key-value pair.
// Returns the value if set succeeds, else returns an error.
func (db *Db) Set(k []byte, v []byte) ([]byte, error) {
	intKSz := int64(len(k))
	intVSz := int64(len(utils.Encode(v)))

	var existingValueForKey []byte
	if db.opts.ShouldWatch {
		existingValueEntryForKey, err := db.Get(k)
		if err != nil {
			existingValueForKey = nil
		} else {
			existingValueForKey = existingValueEntryForKey
		}
	}

	newKeyValueEntry := &KeyValueEntry{
		deleted: DELETED_FLAG_UNSET_VALUE,
		offset:  db.getLastOffset(),
		ksz:     int64(len(k)),
		vsz:     int64(len(utils.Encode(v))),
		size:    int64(StaticChunkSize + intKSz + intVSz),
		k:       k,
		v:       utils.Encode(v),
	}
	newKeyValueEntry.setTTLViaDuration(KEY_EXPIRES_IN_DEFAULT)

	db.mu.Lock()
	defer db.mu.Unlock()
	db.limitDatafileToThreshold(newKeyValueEntry, &Options{})
	newKeyValueEntry.setCRC(newKeyValueEntry.calculateCRC())
	err := db.writeKeyValueEntry(newKeyValueEntry)
	if err != nil {
		return nil, err
	}

	kdValue := NewKeyDirValue(newKeyValueEntry.offset, newKeyValueEntry.size, newKeyValueEntry.tstamp, utils.GetFilenameWithoutExtension(db.activeDataFile))
	_, err = db.setKeyDir(k, *kdValue)

	if err != nil {
		return nil, err
	}

	// do not watch if ShouldWatch is set with options
	if db.opts.ShouldWatch {
		if existingValueForKey == nil {
			db.SendWatchEvent(NewCreateWatcherEvent(k, existingValueForKey, v, nil))
		} else {
			db.SendWatchEvent(NewUpdateWatcherEvent(k, existingValueForKey, v, nil))
		}
	}
	return v, err
}

func (db *Db) SetWithTTL(k []byte, v []byte, ttl time.Duration) ([]byte, error) {
	intKSz := int64(len(k))
	intVSz := int64(len(utils.Encode(v)))

	var existingValueForKey []byte
	if db.opts.ShouldWatch {
		existingValueEntryForKey, err := db.Get(k)
		if err != nil {
			existingValueForKey = nil
		} else {
			existingValueForKey = existingValueEntryForKey
		}
	}

	newKeyValueEntry := &KeyValueEntry{
		deleted: DELETED_FLAG_UNSET_VALUE,
		offset:  db.getLastOffset(),
		ksz:     int64(len(k)),
		vsz:     int64(len(utils.Encode(v))),
		size:    int64(StaticChunkSize + intKSz + intVSz),
		k:       k,
		v:       utils.Encode(v),
	}
	newKeyValueEntry.setTTLViaDuration(ttl)

	db.mu.Lock()
	defer db.mu.Unlock()
	db.limitDatafileToThreshold(newKeyValueEntry, &Options{})
	newKeyValueEntry.setCRC(newKeyValueEntry.calculateCRC())
	err := db.writeKeyValueEntry(newKeyValueEntry)
	if err != nil {
		return nil, err
	}

	kdValue := NewKeyDirValue(newKeyValueEntry.offset, newKeyValueEntry.size, newKeyValueEntry.tstamp, utils.GetFilenameWithoutExtension(db.activeDataFile))
	_, err = db.setKeyDir(k, *kdValue)
	if err != nil {
		return nil, err
	}

	// do not watch if ShouldWatch is set with options
	if db.opts.ShouldWatch {
		if existingValueForKey == nil {
			db.SendWatchEvent(NewCreateWatcherEvent(k, existingValueForKey, v, nil))
		} else {
			db.SendWatchEvent(NewUpdateWatcherEvent(k, existingValueForKey, v, nil))
		}
	}
	return v, err
}

// Deletes a key-value pair.
// Returns error if any.
func (db *Db) Delete(k []byte) ([]byte, error) {
	db.mu.Lock()
	defer db.mu.Unlock()

	err := db.deleteKey(k)
	if db.opts.ShouldWatch {
		db.SendWatchEvent(NewDeleteWatcherEvent(k, nil, nil, nil))
	}
	return k, err
}

func (db *Db) walk(s string, file fs.DirEntry, err error) error {
	if path.Ext(file.Name()) != InactiveKeyValueEntryDataFileSuffix {
		return nil
	}

	path := utils.JoinPaths(db.dirPath, file.Name())
	db.setActiveDataFile(path)
	db.setLastOffset(INITIAL_SEGMENT_OFFSET)

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
		log.Println(err)
		return err
	}

	return nil
}
