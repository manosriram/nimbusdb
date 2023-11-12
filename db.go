package db

import (
	"fmt"
	"os"
	"path"
)

const (
	ACTIVE_SEGMENT_DATAFILE_SUFFIX   = ".dfile"
	SEGMENT_HINTFILE_SUFFIX          = ".hfile"
	INACTIVE_SEGMENT_DATAFILE_SUFFIX = ".idfile"
)

const (
	TEMPFILE_DATAFILE_PATTERN = "*.dfile"
)

type Segment struct {
	fileId        string
	segmentOffset uint64
	tstamp        string
	ksz           uint32
	vsz           uint32
	k             []byte
	v             interface{}
}

type Db struct {
	dirPath string
	keyDir  map[string]*Segment
}

func (db *Db) parseActiveSegmentFile(data []byte) {}

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
		if err != nil {
			return nil, err
		}
		fmt.Println(file)
	}

	// Found files in dir
	for _, file := range dir {
		if file.IsDir() {
			continue
		}

		fileInfo, _ := file.Info()
		if path.Ext(fileInfo.Name()) == ACTIVE_SEGMENT_DATAFILE_SUFFIX {
			fileData, _ := os.ReadFile(dirPath + file.Name())
			db.parseActiveSegmentFile(fileData)
		}
	}

	return db, nil
}
