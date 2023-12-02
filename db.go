package nimbusdb

import (
	"fmt"
	"io"
	"log"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	utils "github.com/manosriram/nimbusdb/utils"
)

const (
	_        = iota // ignore first value by assigning to blank identifier
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
	HomePath                      = "/Users/manosriram/go/src/nimbusdb" // TODO: refactor this
	DatafileThreshold             = 5 * MB
)

const (
	TstampOffset    int64 = 12
	KeySizeOffset   int64 = 16
	ValueSizeOffset int64 = 20
	BlockSize             = 12 + 4 + 4 // tstamp + ksize + vsize
)
const (
	TotalBlockSize int64 = TstampOffset + KeySizeOffset + ValueSizeOffset + BlockSize
)

const (
	KV_EXPIRES_IN = 24 * time.Hour
)

type Segment struct {
	fileID string
	offset int64
	size   int64
	tstamp int64
	ksz    int32
	vsz    int32
	k      []byte
	v      []byte
}

type Options struct {
	IsMerge       bool
	MergeFilePath string
	Path          string
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
	buf = append(buf, utils.Int64ToByte(s.tstamp)...)
	buf = append(buf, utils.Int64ToByte(int64(s.ksz))...)
	buf = append(buf, utils.Int64ToByte(int64(s.vsz))...)

	segmentInBytes = append(segmentInBytes, buf...)
	segmentInBytes = append(segmentInBytes, s.k...)
	segmentInBytes = append(segmentInBytes, s.v...)

	return segmentInBytes
}

type KeyValuePair struct {
	Key   []byte
	Value interface{}
}

type KeyDirValue struct {
	offset int64
	size   int64
	path   string
}

type Db struct {
	mu             sync.Mutex
	dirPath        string
	dataFilePath   string
	activeDataFile string
	lastOffset     int64
	keyDir         map[string]KeyDirValue
	isTest         bool
	opts           *Options
}

// TODO: use dirPath here
func NewDb(dirPath string) *Db {
	keyDir := make(map[string]KeyDirValue, 0)
	db := &Db{
		dirPath: dirPath,
		keyDir:  keyDir,
	}

	return db
}

func (db *Db) LastOffset() int64 {
	return db.lastOffset
}

func (db *Db) setKeyDir(key string, kdValue KeyDirValue) interface{} {
	if key == "" || kdValue.offset < 0 {
		return nil
	}
	db.lastOffset = db.LastOffset() + kdValue.size

	db.keyDir[key] = kdValue
	return db.keyDir[key]
}

func (db *Db) getKeyDir(key string) (*Segment, error) {
	db.mu.Lock()
	defer db.mu.Unlock()
	x, ok := db.keyDir[key]
	if !ok {
		return nil, nil
	}

	v := db.SeekOffsetFromDataFile(x)
	tstampString, err := strconv.ParseInt(fmt.Sprint(v.tstamp), 10, 64)
	if err != nil {
		return nil, err
	}
	hasTimestampExpired := utils.HasTimestampExpired(tstampString)
	if hasTimestampExpired {
		delete(db.keyDir, key)
		return nil, nil
	}
	return v, nil
}

func getSegmentFromOffset(offset int64, data []byte) (*Segment, error) {
	// get timestamp
	if int(offset+BlockSize) > len(data) {
		return nil, fmt.Errorf("exceeded data array length")
	}
	tstamp := data[offset : offset+TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)

	// get key size
	ksz := data[offset+TstampOffset : offset+KeySizeOffset]
	intKsz := utils.ByteToInt64(ksz)

	// get value size
	vsz := data[offset+KeySizeOffset : offset+ValueSizeOffset]
	intVsz := utils.ByteToInt64(vsz)

	if int(offset+ValueSizeOffset+intKsz) > len(data) {
		return nil, fmt.Errorf("exceeded data array length")
	}
	// get key
	k := data[offset+ValueSizeOffset : offset+ValueSizeOffset+intKsz]

	if int(offset+ValueSizeOffset+intKsz+intVsz) > len(data) {
		return nil, fmt.Errorf("exceeded data array length")
	}
	// get value
	v := data[offset+ValueSizeOffset+intKsz : offset+ValueSizeOffset+intKsz+intVsz]

	// make segment
	x := &Segment{
		tstamp: int64(tstamp64Bit),
		ksz:    int32(intKsz),
		vsz:    int32(intVsz),
		k:      k,
		v:      v,
		offset: offset,
		size:   BlockSize + intKsz + intVsz,
	}
	return x, nil
}

func (db *Db) SeekOffsetFromDataFile(kdValue KeyDirValue) *Segment {

	// TODO: improve dfile and idfile recognizing
	f, err := os.Open(fmt.Sprintf("%s/%s.dfile", db.dirPath, kdValue.path))
	if err != nil {
		f, _ = os.Open(fmt.Sprintf("%s/%s.idfile", db.dirPath, kdValue.path))
	}
	data := make([]byte, kdValue.size)
	f.Seek(kdValue.offset, io.SeekCurrent)
	f.Read(data)

	tstamp := data[:TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)

	// get key size
	ksz := data[TstampOffset:KeySizeOffset]
	intKsz := utils.ByteToInt64(ksz)

	// get value size
	vsz := data[KeySizeOffset:ValueSizeOffset]
	intVsz := utils.ByteToInt64(vsz)
	k := data[ValueSizeOffset : ValueSizeOffset+intKsz]

	// get value
	v := data[ValueSizeOffset+intKsz : ValueSizeOffset+intKsz+intVsz]

	return &Segment{
		tstamp: int64(tstamp64Bit),
		ksz:    int32(intKsz),
		vsz:    int32(intVsz),
		k:      k,
		v:      v,
		offset: kdValue.offset,
		size:   kdValue.size,
	}
}

func (db *Db) getActiveFileSegments(filePath string) ([]*Segment, error) {
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

		// segment.fileID = strings.Split(path.Base(filePath), ".")[0]
		segment.fileID = strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
		hasTimestampExpired := utils.HasTimestampExpired(segment.tstamp)
		if !hasTimestampExpired {
			split := strings.Split(filePath, "/")
			fileName := split[len(split)-1]
			kdValue := KeyDirValue{
				offset: segment.offset,
				size:   segment.size,
				path:   strings.Split(fileName, ".")[0],
			}
			db.setKeyDir(string(segment.k), kdValue) // TODO: use Set here?
			segments = append(segments, segment)
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

		// segment.fileID = strings.Split(path.Base(filePath), ".")[0]
		segment.fileID = strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
		hasTimestampExpired := utils.HasTimestampExpired(segment.tstamp)
		if !hasTimestampExpired {
			// fileName := utils.GetFilenameWithoutExtension(filePath)

			fileName := strings.Split(utils.GetFilenameWithoutExtension(filePath), ".")[0]
			kdValue := KeyDirValue{
				offset: segment.offset,
				size:   segment.size,
				path:   fileName,
			}
			db.setKeyDir(string(segment.k), kdValue) // TODO: use Set here?
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
	db.activeDataFile = file.Name()
	db.lastOffset = 0
	if err != nil {
		return err
	}
	return nil
}

func (db *Db) CreateActiveDatafile(dirPath string) error {
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
			oldPath := fmt.Sprintf("%s/%s", dirPath, file.Name())
			newPath := fmt.Sprintf("%s/%s", dirPath, inactiveName)
			os.Rename(oldPath, newPath)
		}
	}

	file, err := os.CreateTemp(dirPath, TempDataFilePattern)
	db.activeDataFile = file.Name()
	db.lastOffset = 0
	if err != nil {
		return err
	}
	return nil
}

func Open(opts *Options) (*Db, error) {
	dirPath := opts.Path
	if dirPath == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}

		dirPath = fmt.Sprintf("%s/%s", home, "nimbusdb")
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
		// no files in path
		// create an active segment file
		err = db.CreateActiveDatafile(dirPath)
		if err != nil {
			return nil, err
		}
	}

	// Found files in dir
	for _, file := range dir {
		if file.IsDir() {
			continue
		}

		fileInfo, _ := file.Info()
		if path.Ext(fileInfo.Name()) == ActiveSegmentDatafileSuffix {
			db.activeDataFile = dirPath + "/" + fileInfo.Name()
			db.parseActiveSegmentFile(db.activeDataFile)
		} else if path.Ext(fileInfo.Name()) == SegmentHintfileSuffix {
			// TODO
			continue
		} else if path.Ext(fileInfo.Name()) == InactiveSegmentDataFileSuffix {
			db.parseActiveSegmentFile(dirPath + file.Name())
		}
	}

	return db, nil
}

func (db *Db) Count() int64 {
	return int64(len(db.keyDir))
}

func (db *Db) All() error {
	for key, value := range db.keyDir {
		v := db.SeekOffsetFromDataFile(value)
		fmt.Printf("key: %s, value: %s, offset: %d\n", key, v.v, v.offset)
	}
	return nil
}

func (db *Db) GetSegmentFromKey(key []byte) (*Segment, error) {
	v, _ := db.getKeyDir(string(key))
	return v, nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	v, _ := db.getKeyDir(string(key))
	if v == nil {
		return nil, fmt.Errorf("key expired or does not exist")
	}
	return v.v, nil
}

func (db *Db) LimitDatafileToThreshold(add int64, opts *Options) {
	var sz os.FileInfo
	var err error
	// fmt.Println(opts.mergeFilePath, db.activeDataFile, db.dirPath)
	sz, err = os.Stat(db.activeDataFile)
	if err != nil {
		log.Fatal(err)
	}
	size := sz.Size()

	if size+add > DatafileThreshold {
		if opts.IsMerge {
			db.CreateInactiveDatafile(db.dirPath)
			os.Remove(opts.MergeFilePath)
			fmt.Println("removed ", opts.MergeFilePath)
		} else {
			db.CreateActiveDatafile(db.dirPath)
		}
	}
}

func (db *Db) Set(kv *KeyValuePair) (interface{}, error) {
	oldValue, _ := db.GetSegmentFromKey(kv.Key)
	if oldValue != nil {
		db.ExpireKey(oldValue.offset)
	}

	intKSz, err := utils.StringToInt(utils.Encode(len(kv.Key)))
	if err != nil {
		return nil, err
	}

	intVSz, err := utils.StringToInt(utils.Encode(len(utils.Encode(kv.Value))))
	if err != nil {
		return nil, err
	}

	encode := utils.Encode
	newSegment := &Segment{
		tstamp: int64(time.Now().Add(KV_EXPIRES_IN).UnixNano()),
		ksz:    int32(len(kv.Key)),
		vsz:    int32(len(encode(kv.Value))),
		k:      encode(kv.Key),
		v:      encode(kv.Value),
		size:   int64(BlockSize + intKSz + intVSz),
	}

	// newSegment.offset = db.LastOffset() + newSegment.size
	db.mu.Lock()
	defer db.mu.Unlock()
	db.LimitDatafileToThreshold(int64(newSegment.size), &Options{})
	err = db.WriteSegment(newSegment)
	if err != nil {
		return nil, err
	}
	kdValue := KeyDirValue{
		offset: newSegment.offset,
		size:   newSegment.size,
		path:   strings.Split(utils.GetFilenameWithoutExtension(db.activeDataFile), ".")[0],
	}
	db.setKeyDir(string(kv.Key), kdValue)
	return kv.Value, err
}

func (db *Db) Sync() error {
	files, err := os.ReadDir(db.dirPath)
	if err != nil {
		return err
	}
	for _, file := range files {
		if path.Ext(file.Name()) != InactiveSegmentDataFileSuffix {
			continue
		}

		path := fmt.Sprintf("%s/%s", db.dirPath, file.Name())
		db.activeDataFile = path
		db.lastOffset = 0

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
	}

	return nil
}
