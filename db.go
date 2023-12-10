package nimbusdb

import (
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
	ActiveSegmentDatafileSuffix   = ".dfile"
	SegmentHintfileSuffix         = ".hfile"
	InactiveSegmentDataFileSuffix = ".idfile"
	TempDataFilePattern           = "*.dfile"
	TempInactiveDataFilePattern   = "*.idfile"
	DefaultDataDir                = "nimbusdb"
	DatafileThreshold             = 5 * MB
)

const (
	DeleteFlagOffset int64 = 1
	TstampOffset           = 11
	KeySizeOffset          = 21
	ValueSizeOffset        = 31
	BlockSize              = 1 + 10 + 10 + 10
	ChunkSize              = 50 * KB
	BTreeDegree            = 10
)
const (
	TotalBlockSize int64 = TstampOffset + KeySizeOffset + ValueSizeOffset + DeleteFlagOffset + BlockSize
)

const (
	KEY_EXPIRES_IN_DEFAULT = 24 * time.Hour

	KEY_NOT_FOUND             = "key expired or does not exist"
	NO_ACTIVE_FILE_OPENED     = "no file opened for writing"
	OFFSET_EXCEEDED_FILE_SIZE = "offset exceeded file size"

	DELETED_FLAG_BYTE_VALUE  = byte(0x31)
	DELETED_FLAG_SET_VALUE   = byte(0x01)
	DELETED_FLAG_UNSET_VALUE = byte(0x00)
)

type Segment struct {
	deleted byte
	fileID  string
	offset  int64
	size    int64 // Equals BlockSize + keysize + valuesize
	tstamp  int64
	ksz     int64
	vsz     int64
	k       []byte
	v       []byte
}

func (s *Segment) BlockSize() int {
	return BlockSize + len(s.k) + len(s.v)
}

func (s *Segment) Key() []byte {
	return s.k
}

func (s *Segment) Value() []byte {
	return s.v
}

func (s *Segment) ToByte() []byte {
	segmentInBytes := make([]byte, 0, s.BlockSize())

	buf := make([]byte, 0)
	buf = append(buf, s.deleted)
	buf = append(buf, utils.Int64ToByte(s.tstamp)...)
	buf = append(buf, utils.Int64ToByte(s.ksz)...)
	buf = append(buf, utils.Int64ToByte(s.vsz)...)

	segmentInBytes = append(segmentInBytes, buf...)
	segmentInBytes = append(segmentInBytes, s.k...)
	segmentInBytes = append(segmentInBytes, s.v...)

	return segmentInBytes
}

type Options struct {
	IsMerge       bool
	MergeFilePath string
	Path          string
}

type KeyValuePair struct {
	Key       []byte
	Value     interface{}
	ExpiresIn time.Duration
}

type KeyDirValue struct {
	offset int64
	size   int64
	path   string
	tstamp int64
}

type Db struct {
	mu                    sync.Mutex
	dirPath               string
	dataFilePath          string
	activeDataFilePointer *os.File
	activeDataFile        string
	lastOffset            atomic.Int64
	keyDir                *BTree
	opts                  *Options
}

func NewDb(dirPath string) *Db {
	db := &Db{
		dirPath: dirPath,
		keyDir: &BTree{
			tree: btree.New(BTreeDegree),
		},
	}

	return db
}

func (db *Db) setLastOffset(v int64) {
	db.lastOffset.Store(v)
}

func (db *Db) LastOffset() int64 {
	return db.lastOffset.Load()
}

func (db *Db) getActiveDataFilePointer() (*os.File, error) {
	if db.activeDataFilePointer == nil {
		return nil, errors.New(NO_ACTIVE_FILE_OPENED)
	}
	return db.activeDataFilePointer, nil
}

func (db *Db) setActiveDataFile(activeDataFile string) error {
	err := db.Close() // close existing active datafile
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

func (db *Db) setKeyDir(key []byte, kdValue KeyDirValue) interface{} {
	if len(key) == 0 || kdValue.offset < 0 {
		return nil
	}
	db.lastOffset.Store(kdValue.offset + kdValue.size)
	db.keyDir.Set(key, kdValue)

	return kdValue
}

func (db *Db) getKeyDir(key []byte) (*Segment, error) {
	x := db.keyDir.Get(key)
	if x == nil {
		return nil, errors.New(KEY_NOT_FOUND)
	}

	v, err := db.seekOffsetFromDataFile(*x)
	if err != nil {
		return nil, err
	}

	tstampString, err := strconv.ParseInt(fmt.Sprint(v.tstamp), 10, 64)
	if err != nil {
		return nil, err
	}
	hasTimestampExpired := utils.HasTimestampExpired(tstampString)
	if hasTimestampExpired {
		db.keyDir.Delete(key)
		return nil, errors.New(KEY_NOT_FOUND)
	}
	return v, nil
}

func getSegmentFromOffset(offset int64, data []byte) (*Segment, error) {
	defer utils.Recover()

	if int(offset+BlockSize) > len(data) {
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

	// make segment
	x := &Segment{
		deleted: deleted,
		tstamp:  int64(tstamp64Bit),
		ksz:     int64(intKsz),
		vsz:     int64(intVsz),
		k:       k,
		v:       v,
		offset:  offset,
		size:    BlockSize + intKsz + intVsz,
	}
	return x, nil
}

func (db *Db) seekOffsetFromDataFile(kdValue KeyDirValue) (*Segment, error) {
	defer utils.Recover()

	// TODO: improve dfile and idfile recognizing
	a := filepath.Join(db.dirPath, fmt.Sprintf("%s.idfile", kdValue.path))
	f, err := os.OpenFile(a, os.O_RDONLY, 0644)
	if err != nil {
		b := filepath.Join(db.dirPath, fmt.Sprintf("%s.dfile", kdValue.path))
		f, err = os.OpenFile(b, os.O_RDONLY, 0644)
		if err != nil {
			return nil, err
		}
	}
	defer f.Close()

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

	return &Segment{
		tstamp: int64(tstamp64Bit),
		ksz:    int64(intKsz),
		vsz:    int64(intVsz),
		k:      k,
		v:      v,
		offset: kdValue.offset,
		size:   kdValue.size,
	}, nil
}

func (db *Db) getActiveFileSegments(filePath string) ([]*Segment, error) {
	defer utils.Recover()

	data, err := utils.ReadFile(filePath)
	if err != nil {
		return nil, err
	}

	segments := make([]*Segment, 0)

	var offset int64 = 0
	for offset < int64(len(data)) {
		segment, err := getSegmentFromOffset(offset, data)
		if err != nil {
			return nil, err
		}

		if segment.deleted != DELETED_FLAG_BYTE_VALUE {
			segment.fileID = strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
			hasTimestampExpired := utils.HasTimestampExpired(segment.tstamp)
			if !hasTimestampExpired {
				split := strings.Split(filePath, "/")
				fileName := split[len(split)-1]
				kdValue := KeyDirValue{
					offset: segment.offset,
					size:   segment.size,
					path:   strings.Split(fileName, ".")[0],
					tstamp: segment.tstamp,
				}
				db.setKeyDir(segment.k, kdValue) // TODO: use Set here?
				segments = append(segments, segment)
			}
		}

		if int(offset+BlockSize) > len(data) {
			offset += segment.size
			break
		}

		offset += segment.size
	}
	return segments, nil
}

func (db *Db) parseActiveSegmentFile(filePath string) error {
	data, err := utils.ReadFile(filePath) // TODO: read in blocks
	if err != nil {
		return err
	}

	var offset int64 = 0
	for offset < int64(len(data)) {
		segment, err := getSegmentFromOffset(offset, data)
		if err != nil {
			return err
		}

		if segment.deleted != DELETED_FLAG_BYTE_VALUE {
			segment.fileID = strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
			hasTimestampExpired := utils.HasTimestampExpired(segment.tstamp)
			if !hasTimestampExpired {
				fileName := strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
				kdValue := KeyDirValue{
					offset: segment.offset,
					size:   segment.size,
					path:   fileName,
					tstamp: segment.tstamp,
				}
				db.setKeyDir(segment.k, kdValue) // TODO: use Set here?
			}
		}

		if int(offset+BlockSize) > len(data) {
			offset += segment.size
			break
		}

		offset += segment.size
	}
	return nil
}

func (db *Db) CreateInactiveDatafile(dirPath string) error {
	file, err := os.CreateTemp(dirPath, TempInactiveDataFilePattern)
	db.setActiveDataFile(file.Name())
	db.setLastOffset(0)
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) CreateActiveDatafile(dirPath string) error {
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
		if extension == ActiveSegmentDatafileSuffix {
			inactiveName := fmt.Sprintf("%s.idfile", strings.Split(file.Name(), ".")[0])

			oldPath := filepath.Join(dirPath, file.Name())
			newPath := filepath.Join(dirPath, inactiveName)
			os.Rename(oldPath, newPath)
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

func (db *Db) Close() error {
	if db.activeDataFilePointer != nil {
		err := db.activeDataFilePointer.Close()
		return err
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
		err = db.CreateActiveDatafile(dirPath)
		if err != nil {
			return nil, err
		}
	}

	filepath.WalkDir(dirPath, func(s string, file fs.DirEntry, e error) error {
		if file.IsDir() {
			return nil
		}

		filePath := utils.JoinPaths(dirPath, file.Name())
		if path.Ext(file.Name()) == ActiveSegmentDatafileSuffix {
			db.setActiveDataFile(filePath)
			db.parseActiveSegmentFile(db.activeDataFile)
		} else if path.Ext(file.Name()) == SegmentHintfileSuffix {
			// TODO
			return nil
		} else if path.Ext(file.Name()) == InactiveSegmentDataFileSuffix {
			db.parseActiveSegmentFile(filePath)
		}

		return nil
	})
	return db, nil
}

func (db *Db) All() []*KeyValuePair {
	return db.keyDir.List()
}

func (db *Db) LimitDatafileToThreshold(add int64, opts *Options) {
	var sz os.FileInfo
	var err error
	f, err := db.getActiveDataFilePointer()
	sz, err = f.Stat()
	if err != nil {
		log.Fatal(err)
	}
	size := sz.Size()

	if size+add > DatafileThreshold {
		if opts.IsMerge {
			db.CreateInactiveDatafile(db.dirPath)
			os.Remove(opts.MergeFilePath)
		} else {
			db.CreateActiveDatafile(db.dirPath)
		}
	}
}

func (db *Db) GetSegmentFromKey(key []byte) (*Segment, error) {
	v, _ := db.getKeyDir(key)
	return v, nil
}

func (db *Db) deleteKey(key []byte) error {
	v := db.keyDir.Get(key)

	a := filepath.Join(db.dirPath, fmt.Sprintf("%s.idfile", v.path))
	f, err := os.OpenFile(a, os.O_WRONLY, 0644)
	if err != nil {
		b := filepath.Join(db.dirPath, fmt.Sprintf("%s.dfile", v.path))
		f, err = os.OpenFile(b, os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
	}
	defer f.Close()

	f.WriteAt([]byte{DELETED_FLAG_SET_VALUE}, v.offset)

	db.keyDir.Delete(key)

	return nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	v, _ := db.getKeyDir(key)
	if v == nil {
		return nil, errors.New(KEY_NOT_FOUND)
	}
	return v.v, nil
}

func (db *Db) Set(kv *KeyValuePair) (interface{}, error) {
	intKSz := int64(len(kv.Key))
	intVSz := int64(len(utils.Encode(kv.Value)))

	newSegment := &Segment{
		deleted: DELETED_FLAG_UNSET_VALUE,
		ksz:     int64(len(kv.Key)),
		vsz:     int64(len(utils.Encode(kv.Value))),
		k:       kv.Key,
		v:       utils.Encode(kv.Value),
		size:    int64(BlockSize + intKSz + intVSz),
		offset:  db.LastOffset(),
	}
	if kv.ExpiresIn > 0 {
		newSegment.tstamp = int64(time.Now().Add(kv.ExpiresIn).UnixNano())
	} else {
		newSegment.tstamp = int64(time.Now().Add(KEY_EXPIRES_IN_DEFAULT).UnixNano())
	}

	db.LimitDatafileToThreshold(int64(newSegment.size), &Options{})
	err := db.WriteSegment(newSegment)
	if err != nil {
		return nil, err
	}
	kdValue := KeyDirValue{
		offset: newSegment.offset,
		size:   newSegment.size,
		path:   strings.Split(utils.GetFilenameWithoutExtension(db.activeDataFile), ".")[0],
		tstamp: newSegment.tstamp,
	}

	db.setKeyDir(kv.Key, kdValue)

	return kv.Value, err
}

func (db *Db) Delete(key []byte) error {
	err := db.deleteKey(key)
	return err
}

func (db *Db) walk(s string, file fs.DirEntry, err error) error {
	if path.Ext(file.Name()) != InactiveSegmentDataFileSuffix {
		return nil
	}

	path := utils.JoinPaths(db.dirPath, file.Name())
	db.setActiveDataFile(path)
	db.setLastOffset(0)

	segments, _ := db.getActiveFileSegments(path)
	if len(segments) == 0 {
		err = os.Remove(path)
		if err != nil {
			return err
		}
	}

	for _, segment := range segments {
		db.LimitDatafileToThreshold(segment.size, &Options{
			IsMerge:       true,
			MergeFilePath: path,
		})
		err := db.WriteSegment(segment)
		if err != nil {
			return err
		}
	}
	return nil
}

func (db *Db) Sync() error {
	err := filepath.WalkDir(db.dirPath, db.walk)
	if err != nil {
		fmt.Println(err)
		return err
	}

	return nil
}
