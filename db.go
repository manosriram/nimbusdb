package nimbusdb

import (
	"encoding/binary"
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	utils "github.com/manosriram/nimbusdb/utils"
)

const (
	ActiveSegmentDatafileSuffix   = ".dfile"
	SegmentHintfileSuffix         = ".hfile"
	InactiveSegmentDataFileSuffix = ".idfile"
)

const (
	TstampOffset    = 8
	KeySizeOffset   = 12
	ValueSizeOffset = 16
	BlockSize       = 8 + 1 + 1 // tstamp + ksize + vsize
)

const (
	TempDataFilePattern = "*.dfile"
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

	buf := make([]byte, 0, s.BlockSize())
	buffer := make([]byte, 8)
	binary.LittleEndian.PutUint64(buffer, uint64(s.tstamp))
	buf = append(buf, buffer...)

	buffer = make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, uint32(s.ksz))
	buf = append(buf, buffer...)

	buffer = make([]byte, 4)
	binary.LittleEndian.PutUint32(buffer, uint32(s.vsz))
	buf = append(buf, buffer...)

	fmt.Println(len(buf))
	fmt.Println(buf)
	fmt.Println(string(buf))

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
	db.keyDir[key] = value
	return value
}

func (db *Db) getKeyDir(key string) (*Segment, error) {
	v := db.keyDir[key]
	if v == nil {
		return nil, nil
	}

	tstampString, err := strconv.ParseInt(string(v.tstamp), 10, 64)
	if err != nil {
		return nil, err
	}
	tstamp := time.Unix(0, tstampString).UnixNano()
	now := time.Now().UnixNano()
	if tstamp < now {
		fmt.Println("key expired! deleting from keyDir")
		delete(db.keyDir, key)
		return nil, nil
	}
	return db.keyDir[key], nil
}

func getSegmentFromOffset(offset int, data []byte) (*Segment, error) {
	b := make([]byte, 8)
	tstamp := data[offset : offset+8]
	tstamp64Bit := binary.LittleEndian.Uint64(tstamp)
	binary.LittleEndian.PutUint64(b, tstamp64Bit)
	fmt.Println(string(b))

	ksz := data[offset+TstampOffset : offset+KeySizeOffset]
	intKsz := binary.LittleEndian.Uint16(ksz)

	vsz := data[offset+KeySizeOffset : offset+ValueSizeOffset]
	intVsz := binary.LittleEndian.Uint16(vsz)

	intKSz, err := utils.StringToInt(ksz)
	if err != nil {
		return nil, err
	}

	intVSz, err := utils.StringToInt(vsz)
	if err != nil {
		return nil, err
	}

	k := data[offset+ValueSizeOffset : offset+ValueSizeOffset+intKSz]
	v := data[offset+ValueSizeOffset+intKSz : offset+ValueSizeOffset+intKSz+intVSz]

	x := &Segment{
		tstamp: int64(tstamp64Bit),
		ksz:    int32(intKsz),
		vsz:    int32(intVsz),
		k:      k,
		v:      v,
		offset: offset,
		size:   BlockSize + intKSz + intVSz,
	}
	fmt.Println(x)
	return nil, nil
}

func (db *Db) parseActiveSegmentFile(filePath string) error {
	data, err := utils.ReadFile(filePath)
	if err != nil {
		return err
	}

	var offset int = 0
	for offset < len(data) {
		segment, _ := getSegmentFromOffset(offset, data)
		fmt.Println(segment)
		break
		// if err != nil {
		// return err
		// }

		// segment.fileID = strings.Split(path.Base(filePath), ".")[0]
		// v, _ := db.getKeyDir(string(segment.k))
		// // fmt.Println("getting ", string(segment.k))
		// if v != nil {
		// db.ExpireKey(offset)
		// }
		// // fmt.Println(string(segment.tstamp), string(segment.vsz), string(segment.ksz), string(segment.k), string(segment.v), segment.fileID)
		// db.setKeyDir(string(segment.k), segment)
		// offset += segment.size
		// db.lastOffset = offset
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

func (db *Db) All() {
	fmt.Println("all in")
	for _, v := range db.keyDir {
		fmt.Printf("key: %s, value: %s\n", v.k, v.v)
	}
}

func (db *Db) GetSegmentFromKey(key []byte) (*Segment, error) {
	v, _ := db.getKeyDir(string(key))
	return v, nil
}

func (db *Db) Get(key []byte) ([]byte, error) {
	v, _ := db.getKeyDir(string(key))
	return v.v, nil
}

func (db *Db) Set(kv *KeyValuePair) (interface{}, error) {
	oldValue, _ := db.GetSegmentFromKey(kv.Key)
	fmt.Println(oldValue)
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
		tstamp: int64(time.Now().Add(1 * time.Hour).UnixNano()),
		ksz:    int32(len(kv.Key)),
		vsz:    int32(len(encode(kv.Value))),
		k:      encode(kv.Key),
		v:      encode(kv.Value),
		size:   BlockSize + intKSz + intVSz,
		offset: db.LastOffset(),
	}

	err = db.WriteSegment(newSegment)
	if err != nil {
		fmt.Println("err = ", err)
		return nil, err
	}
	db.setKeyDir(string(kv.Key), newSegment)
	db.lastOffset = db.LastOffset() + BlockSize + intKSz + intVSz
	// fmt.Println(db.keyDir)
	return kv.Value, err
}
