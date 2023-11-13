package nimbusdb

import (
	"fmt"
	"os"
	"path"
	"strconv"
	"time"

	utils "github.com/manosriram/nimbusdb/utils"
)

const (
	ACTIVE_SEGMENT_DATAFILE_SUFFIX   = ".dfile"
	SEGMENT_HINTFILE_SUFFIX          = ".hfile"
	INACTIVE_SEGMENT_DATAFILE_SUFFIX = ".idfile"
)

const (
	TEMPFILE_DATAFILE_PATTERN = "*.dfile"
)

type VTYPE string

const (
	INT    VTYPE = "i"
	STRING VTYPE = "s"
	JSON   VTYPE = "j"
)

type Segment struct {
	fileId        string
	segmentOffset uint64
	segmentSize   uint64
	tstamp        []byte
	ksz           []byte
	vsz           []byte
	k             []byte
	v             []byte
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

func (db *Db) parseActiveSegmentFile(data []byte) {
	index := 0

	for index < len(data) {
		tstamp := data[index : index+19]
		ksz := data[index+19 : index+20]
		vsz := data[index+20 : index+21]
		intksz, _ := strconv.Atoi(string(ksz))
		intvsz, _ := strconv.Atoi(string(vsz))
		k := data[index+21 : index+21+intksz]
		v := data[index+21+intksz : index+21+intksz+intvsz]
		fmt.Println(string(tstamp), string(vsz), string(ksz), string(k), string(v))
		index += 21 + intksz + intvsz
	}
}

func Open(dirPath string) (*Db, error) {
	db := &Db{
		dirPath: dirPath,
	}

	dir, err := os.ReadDir(dirPath)
	if err != nil {
		return nil, err
	}

	// Empty path, starting new
	if len(dir) == 0 {
		// no files in path
		// create an active segment file
		file, err := os.CreateTemp(dirPath, TEMPFILE_DATAFILE_PATTERN)
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
		if path.Ext(fileInfo.Name()) == ACTIVE_SEGMENT_DATAFILE_SUFFIX {
			fileData, _ := os.ReadFile(dirPath + file.Name())
			db.activeDataFile = dirPath + fileInfo.Name()
			// TODO
			db.parseActiveSegmentFile(fileData)
		} else if path.Ext(fileInfo.Name()) == SEGMENT_HINTFILE_SUFFIX {
			// TODO
			continue
		} else if path.Ext(fileInfo.Name()) == INACTIVE_SEGMENT_DATAFILE_SUFFIX {
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
