package nimbusdb

import (
	"os"
	"path/filepath"
)

type Segment struct {
	closed             bool
	currentBlockNumber int64
	currentBlockOffset int64
	path               string
	blocks             map[int64]*BlockOffsetPair
	fp                 *os.File
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

func (db *Db) getSegmentBlockOffset(path string) int64 {
	return db.segments[path].currentBlockOffset
}

func (db *Db) getSegmentBlockNumber(path string) int64 {
	return db.segments[path].currentBlockNumber
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

func (db *Db) setSegmentBlockNumber(path string, blockNumber int64) {
	segment := db.getSegment(path)
	segment.currentBlockNumber = blockNumber
	db.setSegment(path, segment)
}

func (db *Db) setSegmentBlockOffset(path string, blockOffset int64) {
	segment := db.getSegment(path)
	segment.currentBlockOffset = blockOffset
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
