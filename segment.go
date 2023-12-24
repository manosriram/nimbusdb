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

func (db *Db) setSegment(path string, segment *Segment) {
	db.segments[path] = segment
}

func (db *Db) getSegment(path string) (*Segment, bool) {
	segment, ok := db.segments[path]
	return segment, ok
}

func (seg *Segment) getBlockOffset() int64 {
	return seg.currentBlockOffset
}

func (seg *Segment) getBlockNumber() int64 {
	return seg.currentBlockNumber
}

func (seg *Segment) getFp() *os.File {
	return seg.fp
}

func (seg *Segment) getPath() string {
	return seg.path
}

func (seg *Segment) setPath(path string) {
	seg.path = path
}

func (seg *Segment) setFp(fp *os.File) {
	seg.fp = fp
}

func (seg *Segment) closeFp() error {
	if !seg.closed {
		err := seg.fp.Close()
		if err != nil {
			return err
		}
	}
	seg.closed = true
	return nil
}

func (seg *Segment) setBlockNumber(blockNumber int64) {
	seg.currentBlockNumber = blockNumber
}

func (seg *Segment) setBlockOffset(blockOffset int64) {
	seg.currentBlockOffset = blockOffset
}

func (seg *Segment) setBlock(blockNumber int64, block *BlockOffsetPair) {
	seg.blocks[blockNumber] = block
}

func (db *Db) getSegmentFilePointerFromPath(keyDirPath string) (*os.File, error) {
	path := filepath.Join(db.dirPath, keyDirPath)
	f, err := os.OpenFile(path, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return f, nil
}

func (db *Db) updateSegment(kdValue *KeyDirValue, segment *Segment) (*Segment, error) {
	segmentBlock, ok := db.getSegmentBlock(kdValue.path, segment.currentBlockNumber)
	if !ok {
		return nil, ERROR_CANNOT_READ_FILE
	}
	segmentBlock.endOffset = kdValue.offset + kdValue.size
	segment.setBlock(segment.currentBlockNumber, segmentBlock)
	if segment.currentBlockOffset+kdValue.size <= BlockSize {
		kdValue.blockNumber = segment.currentBlockNumber
		segment.setBlockOffset(segment.getBlockOffset() + kdValue.size)
	} else {
		segment.currentBlockNumber += 1
		segment.blocks[segment.currentBlockNumber] = &BlockOffsetPair{
			startOffset: kdValue.offset,
			endOffset:   kdValue.offset + kdValue.size,
			filePath:    kdValue.path,
		}
		kdValue.blockNumber = segment.currentBlockNumber
		segment.setBlockOffset(kdValue.size)
	}
	db.setSegment(kdValue.path, segment)
	return segment, nil
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
