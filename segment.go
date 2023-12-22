package nimbusdb

import (
	"os"
	"path/filepath"
)

type Segment struct {
	path               string
	fp                 *os.File
	closed             bool
	blocks             map[int64]*BlockOffsetPair
	currentBlockNumber int64
	currentBlockOffset int64
}

func (db *Db) getSegmentBlock(path string, blockNumber int64) (*BlockOffsetPair, bool) {
	segment, ok := db.segments[path]
	if !ok {
		return nil, ok
	}
	block, ok := segment.blocks[blockNumber]
	if !ok {
		return nil, ok
	}
	return block, true
}

func (db *Db) getSegment(path string) *Segment {
	return db.segments[path]
}

func (db *Db) setSegment(path string, segment *Segment) {
	db.segments[path] = segment
}

func (db *Db) setSegmentPath(segmentPath string, path string) {
	segment := db.getSegment(segmentPath)
	segment.path = path
	db.setSegment(path, segment)
}

func (db *Db) setSegmentFp(path string, fp *os.File) {
	segment := db.getSegment(path)
	segment.fp = fp
	db.setSegment(path, segment)
}

func (db *Db) getSegmentFilePointerFromPath(keyDirPath string) (*os.File, error) {
	path := filepath.Join(db.dirPath, keyDirPath)
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return f, nil
}
