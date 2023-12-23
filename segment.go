package nimbusdb

import (
	"os"
	"path/filepath"
)

// Segment represents an entire file. It is divided into Blocks.
// Each Segment is a collection of Blocks of size 32KB. A file pointer is kept opened for reading purposes.
// closed represents the state of the Segment's file pointer.
type Segment struct {
	closed             bool
	currentBlockNumber int64
	currentBlockOffset int64
	path               string
	blocks             map[int64]*BlockOffsetPair
	fp                 *os.File
}

// BlockOffsetPair contains metadata about the Block. The start and ending offsets of the Block, and the path.
type BlockOffsetPair struct {
	startOffset int64
	endOffset   int64
	filePath    string
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

func (db *Db) setSegmentBlock(path string, blockNumber int64, block *BlockOffsetPair) {
	segment := db.getSegment(path)
	segment.blocks[blockNumber] = block
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

func (db *Db) updateSegment(kdValue *KeyDirValue, segment *Segment) error {
	segmentBlock, ok := db.getSegmentBlock(kdValue.path, segment.currentBlockNumber)
	if !ok {
		return ERROR_CANNOT_READ_FILE
	}
	segmentBlock.endOffset = kdValue.offset + kdValue.size
	db.setSegmentBlock(kdValue.path, segment.currentBlockNumber, segmentBlock)
	if segment.currentBlockOffset+kdValue.size <= BlockSize {
		kdValue.blockNumber = segment.currentBlockNumber
		db.setSegmentBlockOffset(kdValue.path, db.getSegmentBlockOffset(kdValue.path)+kdValue.size)
	} else {
		segment.currentBlockNumber += 1
		segment.blocks[segment.currentBlockNumber] = &BlockOffsetPair{
			startOffset: kdValue.offset,
			endOffset:   kdValue.offset + kdValue.size,
			filePath:    kdValue.path,
		}
		kdValue.blockNumber = segment.currentBlockNumber
		db.setSegmentBlockOffset(kdValue.path, kdValue.size)
	}
	db.setSegment(kdValue.path, segment)
	return nil
}

func createNewSegment(kdValue *KeyDirValue) *Segment {
	return &Segment{
		blocks: map[int64]*BlockOffsetPair{
			0: {startOffset: kdValue.offset,
				endOffset: kdValue.offset + kdValue.size,
				filePath:  kdValue.path,
			},
		},
		path:               kdValue.path,
		currentBlockNumber: 0,
		currentBlockOffset: 0,
	}
}
