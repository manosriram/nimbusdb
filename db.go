package nimbusdb

import (
	"fmt"
	"os"
	"path"
	"time"

	utils "github.com/manosriram/nimbusdb/utils"
)

const (
	ActiveSegmentDatafileSuffix   = ".dfile"
	SegmentHintfileSuffix         = ".hfile"
	InactiveSegmentDataFileSuffix = ".idfile"
)

const (
	TstampOffset    = 19
	KeySizeOffset   = 20
	ValueSizeOffset = 21
	BlockSize       = 19 + 1 + 1 // tstamp + ksize + vsize
)

const (
	TempDataFilePattern = "*.dfile"
)

// type VTYPE string

type Segment struct {
	fileID string
	offset int
	size   int
	tstamp []byte
	ksz    []byte
	vsz    []byte
	k      []byte
	v      []byte
}

func (s *Segment) BlockSize() int {
	return len(s.tstamp) + len(s.ksz) + len(s.vsz) + len(s.k) + len(s.v)
}

func (s *Segment) ToByte() []byte {
	segmentInBytes := make([]byte, 0, s.BlockSize())

	segmentInBytes = append(segmentInBytes, s.tstamp...)
	segmentInBytes = append(segmentInBytes, s.ksz...)
	segmentInBytes = append(segmentInBytes, s.vsz...)
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
	keyDir         map[string]*Segment
}

func NewDb(dirPath string) *Db {
	keyDir := make(map[string]*Segment, 0)
	return &Db{
		dirPath: dirPath,
		keyDir:  keyDir,
	}
}

func (db *Db) setKeyDir(key string, value *Segment) interface{} {
	db.keyDir[key] = value
	return value
}

func getSegmentFromOffset(offset int, data []byte) (*Segment, error) {
	tstamp := data[offset : offset+TstampOffset]
	ksz := data[offset+TstampOffset : offset+KeySizeOffset]
	vsz := data[offset+KeySizeOffset : offset+ValueSizeOffset]

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

	return &Segment{
		tstamp: tstamp,
		ksz:    ksz,
		vsz:    vsz,
		k:      k,
		v:      v,
		offset: offset,
		size:   BlockSize + intKSz + intVSz,
	}, nil
}

func (db *Db) parseActiveSegmentFile(data []byte) error {
	var offset int = 0

	for offset < len(data) {
		segment, err := getSegmentFromOffset(offset, data)
		if err != nil {
			return err
		}
		offset += segment.size

		db.setKeyDir(string(segment.k), segment)
		fmt.Println(string(segment.tstamp), string(segment.vsz), string(segment.ksz), string(segment.k), string(segment.v))
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
			fileData, _ := os.ReadFile(dirPath + file.Name())
			db.activeDataFile = dirPath + fileInfo.Name()
			// TODO
			db.parseActiveSegmentFile(fileData)
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

func (db *Db) Set(kv *KeyValuePair) (interface{}, error) {
	encode := utils.Encode
	newSegment := &Segment{
		tstamp: encode(time.Now().UnixNano()),
		ksz:    encode(len(kv.Key)),
		vsz:    encode(len(encode(kv.Value))),
		k:      encode(kv.Key),
		v:      encode(kv.Value),
	}

	err := db.Write(newSegment)
	return kv.Value, err
}
