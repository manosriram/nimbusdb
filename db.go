package nimbusdb

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	utils "github.com/manosriram/nimbusdb/utils"
)

const (
	ActiveSegmentDatafileSuffix   = ".dfile"
	SegmentHintfileSuffix         = ".hfile"
	InactiveSegmentDataFileSuffix = ".idfile"
	TempDataFilePattern           = "*.dfile"
)

const (
	TstampOffset    = 12
	KeySizeOffset   = 16
	ValueSizeOffset = 20
	BlockSize       = 8 + 1 + 1 // tstamp + ksize + vsize
)

const (
	KV_EXPIRES_IN = 24 * time.Hour
)

// type VTYPE string

type Segment struct {
	fileID string
	offset int
	size   int
	tstamp int64
	ksz    int32
	vsz    int32
	k      []byte
	v      []byte
}

func (s *Segment) BlockSize() int {
	return BlockSize + len(s.k) + len(s.v)
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

type Db struct {
	dirPath        string
	activeDataFile string
	lastOffset     int
	keyDir         map[string]*Segment
}

func NewDb(dirPath string) *Db {
	keyDir := make(map[string]*Segment, 0)
	return &Db{
		dirPath: dirPath,
		keyDir:  keyDir,
	}
}

func (db *Db) LastOffset() int {
	return db.lastOffset
}

func (db *Db) setKeyDir(key string, value *Segment) interface{} {
	if key == "" || len(value.k) == 0 || len(value.v) == 0 {
		return nil
	}
	db.keyDir[key] = value
	return value
}

func (db *Db) getKeyDir(key string) (*Segment, error) {
	v := db.keyDir[key]
	if v == nil {
		return nil, nil
	}

	tstampString, err := strconv.ParseInt(fmt.Sprint(v.tstamp), 10, 64)
	if err != nil {
		return nil, err
	}
	hasTimestampExpired := utils.HasTimestampExpired(tstampString)
	if hasTimestampExpired {
		delete(db.keyDir, key)
		return nil, nil
	}
	return db.keyDir[key], nil
}

func getSegmentFromOffset(offset int, data []byte) (*Segment, error) {
	// get timestamp
	tstamp := data[offset : offset+TstampOffset]
	tstamp64Bit := utils.ByteToInt64(tstamp)

	// get key size
	ksz := data[offset+TstampOffset : offset+KeySizeOffset]
	intKsz := utils.ByteToInt64(ksz)

	// get value size
	vsz := data[offset+KeySizeOffset : offset+ValueSizeOffset]
	intVsz := utils.ByteToInt64(vsz)

	// get key
	k := data[offset+ValueSizeOffset : offset+ValueSizeOffset+int(intKsz)]

	// get value
	v := data[offset+ValueSizeOffset+int(intKsz) : offset+ValueSizeOffset+int(intKsz)+int(intVsz)]

	// make segment
	x := &Segment{
		tstamp: int64(tstamp64Bit),
		ksz:    int32(intKsz),
		vsz:    int32(intVsz),
		k:      k,
		v:      v,
		offset: offset,
		size:   BlockSize + int(intKsz) + int(intVsz),
	}
	return x, nil
}

func (db *Db) parseActiveSegmentFile(filePath string) error {
	data, err := utils.ReadFile(filePath)
	if err != nil {
		return err
	}

	var offset int = 0
	for offset <= len(data) {
		segment, err := getSegmentFromOffset(offset, data)
		if err != nil {
			return err
		}

		// v, _ := db.getKeyDir(string(segment.k))
		// if v != nil {
		// // db.ExpireKey(offset)
		// // offset += segment.size
		// // db.lastOffset = offset
		// continue
		// }

		segment.fileID = strings.Split(path.Base(filePath), ".")[0]
		hasTimestampExpired := utils.HasTimestampExpired(segment.tstamp)
		if !hasTimestampExpired {
			db.setKeyDir(string(segment.k), segment) // TODO: use Set here?
		}
		offset += segment.size
		db.lastOffset = offset
		// break
	}
	return nil
}

func Open(dirPath string) (*Db, error) {
	db := NewDb(dirPath)

	dir, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Empty path, starting new
	if len(dir) == 0 {
		// no files in path
		// create an active segment file
		file, err := os.CreateTemp(dirPath, TempDataFilePattern)
		db.activeDataFile = file.Name()
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
			// fileData, _ := os.ReadFile(dirPath + file.Name())
			db.activeDataFile = dirPath + fileInfo.Name()
			db.parseActiveSegmentFile(dirPath + file.Name())
		} else if path.Ext(fileInfo.Name()) == SegmentHintfileSuffix {
			// TODO
			continue
		} else if path.Ext(fileInfo.Name()) == InactiveSegmentDataFileSuffix {
			// TODO
			continue
		}
	}

	return db, nil
}

func (db *Db) All() error {
	for _, pair := range db.keyDir {
		value, err := db.Get(pair.k)
		if err != nil {
			// fmt.Printf("%s", err.Error())
			continue
		}
		fmt.Printf("key: %s, value: %s\n", pair.k, value)
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
		size:   BlockSize + intKSz + intVSz,
		offset: db.LastOffset(),
	}

	err = db.WriteSegment(newSegment)
	if err != nil {
		return nil, err
	}
	db.setKeyDir(string(kv.Key), newSegment)
	db.lastOffset = db.LastOffset() + BlockSize + intKSz + intVSz
	return kv.Value, err
}
