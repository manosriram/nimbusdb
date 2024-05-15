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

func NewKeyDirValue(offset, size, tstamp int64, path string) *KeyDirValue {
	return &KeyDirValue{
		offset: offset,
		size:   size,
		tstamp: tstamp,
		path:   path,
	}
}

type Db struct {
	dirPath string
	closed  bool

	reader                  *os.File
	writer                  *os.File
	mergeWriter             *os.File
	activeMergeDataFileName string
	activeDataFileName      string

	keyDir     *BTree
	opts       *Options
	lastOffset atomic.Int64
	mu         sync.RWMutex
	segments   map[string]*Segment
	lru        *expirable.LRU[int64, *Block]
	watcher    chan WatcherEvent
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

func (db *Db) setActiveDataFileReader(reader *os.File) error {
	if db.reader != nil {
		db.closeActiveDataFileReader()
	}
	db.reader = reader
	return nil
}

func (db *Db) setActiveDataFileWriter(writer *os.File) error {
	if db.writer != nil {
		db.closeActiveDataFileWriter()
	}
	db.writer = writer
	return nil
}

func (db *Db) getActiveDataFileReader() (*os.File, error) {
	if db.reader == nil {
		return nil, ERROR_DATA_FILE_READER_NOT_OPEN
	}
	return db.reader, nil
}

func (db *Db) getActiveDataFileWriter() (*os.File, error) {
	if db.writer == nil {
		return nil, ERROR_DATA_FILE_WRITER_NOT_CLOSED
	}
	return db.writer, nil
}

func (db *Db) getActiveMergeFileWriter() (*os.File, error) {
	if db.mergeWriter == nil {
		return nil, ERROR_NO_ACTIVE_MERGE_FILE_OPENED
	}
	return db.mergeWriter, nil
}

func (db *Db) closeActiveMergeDataFilePointer() error {
	if db.mergeWriter != nil {
		return db.mergeWriter.Close()
	}
	return nil
}

func (db *Db) closeActiveDataFileReader() error {
	if db.reader != nil {
		return db.reader.Close()
	}
	return nil
}

func (db *Db) closeActiveDataFileWriter() error {
	if db.writer != nil {
		return db.writer.Close()
	}
	return nil
}

func (db *Db) setActiveDataFile(activeDataFile string) error {
	err := db.closeActiveDataFileWriter()
	if err != nil {
		return err
	}

	writer, err := os.OpenFile(activeDataFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}
	db.setActiveDataFileWriter(writer)

	reader, err := os.OpenFile(activeDataFile, os.O_RDONLY, 0644)
	if err != nil {
		return err
	}
	db.setActiveDataFileReader(reader)

	db.activeDataFileName = activeDataFile
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
		newSegment.writer = fp
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
			if bytes.Compare(key, entry.key) == 0 {
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
			keyValueEntry.fileId = utils.GetFilenameWithoutExtension(filePath)
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
				_, err := db.setKeyDir(keyValueEntry.key, kdValue) // TODO: use Set here?
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

func (db *Db) getActiveKeyValueEntriesInFile(filePath string) ([]*ActiveKeyValueOffset, []byte, error) {
	data, err := utils.ReadFile(filePath) // TODO: read in blocks
	if err != nil {
		return nil, nil, err
	}

	var validKeys []*ActiveKeyValueOffset
	var previousOffset int64 = 0
	var offset int64 = 0
	for offset < int64(len(data)) {
		keyValueEntry, err := getKeyValueEntryFromOffsetViaData(offset, data)
		if err != nil && !errors.Is(err, ERROR_KEY_NOT_FOUND) {
			return nil, nil, err
		} else if err != nil && errors.Is(err, ERROR_KEY_NOT_FOUND) {
			previousOffset = offset
			if int(offset+StaticChunkSize) > len(data) {
				offset += keyValueEntry.size
				break
			}
			offset += keyValueEntry.size
			continue
		}

		keyValueEntry.fileId = utils.GetFilenameWithoutExtension(filePath)
		hasTimestampExpired := utils.HasTimestampExpired(keyValueEntry.tstamp)
		if !hasTimestampExpired {
			validKeys = append(validKeys, &ActiveKeyValueOffset{
				Startoffset: previousOffset,
				Endoffset:   offset,
			})
		}

		previousOffset = offset
		if int(offset+StaticChunkSize) > len(data) {
			offset += keyValueEntry.size
			break
		}

		offset += keyValueEntry.size
	}
	return validKeys, data, nil
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
			keyValueEntry.fileId = utils.GetFilenameWithoutExtension(filePath)
			hasTimestampExpired := utils.HasTimestampExpired(keyValueEntry.tstamp)
			if !hasTimestampExpired {
				fileName := utils.GetFilenameWithoutExtension(filePath)
				kdValue := KeyDirValue{
					offset: keyValueEntry.offset,
					size:   keyValueEntry.size,
					path:   fileName,
					tstamp: keyValueEntry.tstamp,
				}
				_, err := db.setKeyDir(keyValueEntry.key, kdValue) // TODO: use Set here?
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
			segment.setWriter(fp)
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
			db.parseActiveKeyValueEntryFile(db.activeDataFileName)
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
	err := db.closeActiveDataFileReader()
	if err != nil {
		return err
	}

	err = db.closeActiveDataFileWriter()
	if err != nil {
		return err
	}

	for _, segment := range db.segments {
		err := segment.closeWriter()
		if err != nil {
			return err
		}
	}
	db.closed = true
	return nil
}

func (db *Db) All() []*KeyValuePair {
	return db.keyDir.List()
}

func (db *Db) limitDatafileToThreshold(newKeyValueEntry *KeyValueEntry) {
	var readerInfo os.FileInfo
	var err error
	reader, err := db.getActiveDataFileReader()
	readerInfo, err = reader.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := readerInfo.Size()

	if size+newKeyValueEntry.size > DatafileThreshold {
		db.createActiveDatafile(db.dirPath)
		newKeyValueEntry.offset = INITIAL_KEY_VALUE_ENTRY_OFFSET
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
	writer := segment.getWriter()
	writer.WriteAt([]byte{DELETED_FLAG_SET_VALUE}, v.offset)
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
	return v.value, nil
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
		deleted:   DELETED_FLAG_UNSET_VALUE,
		offset:    db.getLastOffset(),
		keySize:   int64(len(k)),
		valueSize: int64(len(utils.Encode(v))),
		size:      int64(StaticChunkSize + intKSz + intVSz),
		key:       k,
		value:     utils.Encode(v),
	}
	newKeyValueEntry.setTTLViaDuration(KEY_EXPIRES_IN_DEFAULT)

	db.mu.Lock()
	defer db.mu.Unlock()
	db.limitDatafileToThreshold(newKeyValueEntry)
	newKeyValueEntry.setCRC(newKeyValueEntry.calculateCRC())
	err := db.writeKeyValueEntry(newKeyValueEntry)
	if err != nil {
		return nil, err
	}

	kdValue := NewKeyDirValue(newKeyValueEntry.offset, newKeyValueEntry.size, newKeyValueEntry.tstamp, utils.GetFilenameWithoutExtension(db.activeDataFileName))
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
	keySizeInInt64 := int64(len(k))
	valueSizeInInt64 := int64(len(utils.Encode(v)))

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
		deleted:   DELETED_FLAG_UNSET_VALUE,
		offset:    db.getLastOffset(),
		keySize:   int64(len(k)),
		valueSize: int64(len(utils.Encode(v))),
		size:      int64(StaticChunkSize + keySizeInInt64 + valueSizeInInt64),
		key:       k,
		value:     utils.Encode(v),
	}
	newKeyValueEntry.setTTLViaDuration(ttl)

	db.mu.Lock()
	defer db.mu.Unlock()
	db.limitDatafileToThreshold(newKeyValueEntry)
	newKeyValueEntry.setCRC(newKeyValueEntry.calculateCRC())
	err := db.writeKeyValueEntry(newKeyValueEntry)
	if err != nil {
		return nil, err
	}

	kdValue := NewKeyDirValue(newKeyValueEntry.offset, newKeyValueEntry.size, newKeyValueEntry.tstamp, utils.GetFilenameWithoutExtension(db.activeDataFileName))
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

// Sets db.activeMergeDataFilePointer to a newly created .swp file
// Sets db.activeDataFile to above pointer's name string
func (db *Db) initMergeDataFileWriter() {
	file, err := os.CreateTemp(db.dirPath, SwapFilePattern)
	if err != nil {
		fmt.Println("error init merge ", err.Error())
	}
	db.mergeWriter = file
	db.activeMergeDataFileName = file.Name()
}

// For each file inside dirPath
func (db *Db) walk(s string, file fs.DirEntry, err error) error {
	if db.mergeWriter == nil {
		db.initMergeDataFileWriter()
	}
	mergeWriter, err := db.getActiveMergeFileWriter()
	if err != nil {
		return err
	}

	if path.Ext(file.Name()) != InactiveKeyValueEntryDataFileSuffix {
		return nil
	}

	oldPath := utils.JoinPaths(db.dirPath, file.Name())
	newPath := utils.GetSwapFilePath(db.dirPath, db.mergeWriter.Name())
	keys, d, err := db.getActiveKeyValueEntriesInFile(oldPath)
	if err != nil {
		return err
	}

	// If there are no active keys in the current .idfile,
	// remove the created paths and return
	if len(keys) == 0 {
		os.Remove(oldPath)
		os.Remove(newPath)
		return nil
	}

	info, err := mergeWriter.Stat()
	if err != nil {
		return err
	}

	// For every active key, write it to db.activeMergeDataFile
	// If the current iteration's key size > DatafileThreshold,
	// close the current activeMergeDataFilePointer,
	// rename the .swp file to .idfile,
	// and create a new swap file
	for _, z := range keys {
		if (z.Endoffset-z.Startoffset)+info.Size() > DatafileThreshold {
			db.closeActiveMergeDataFilePointer()
			swapFilename := strings.Split(newPath, ".")[0]
			err = os.Rename(newPath, fmt.Sprintf("%s.idfile", swapFilename))
			if err != nil {
				return err
			}
			db.initMergeDataFileWriter()
		}
		mergeWriter.WriteAt(d[z.Startoffset:z.Endoffset], z.Startoffset)
	}
	return nil
}

// Syncs the database. Will remove all expired/deleted keys from disk.
// Since items are removed, disk usage will reduce.
func (db *Db) merge() error {
	defer db.closeActiveMergeDataFilePointer()
	err := filepath.WalkDir(db.dirPath, db.walk)
	if err != nil {
		return err
	}

	// After finishing the merge process, convert left over swap files (if any)
	// to .idfile
	files, err := filepath.Glob(filepath.Join(db.dirPath, "*.swp"))
	if err != nil {
		return err
	}
	for _, file := range files {
		ff := strings.Split(file, ".")[0]
		err := os.Rename(file, fmt.Sprintf("%s.idfile", ff))
		if err != nil {
			return err
		}
	}

	return nil
}

func (db *Db) RunCompaction() error {
	// TODO:
	// 1. Compaction
	// 2. HintFiles
	if err := db.merge(); err != nil {
		return err
	}
	return nil
}
