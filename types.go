package nimbusdb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/segmentio/ksuid"
)

const (
	_        = iota
	KB int64 = 1 << (10 * iota)
	MB
	GB
	TB
	PB
	EB
)

const (
	ActiveKeyValueEntryDatafileSuffix   = ".dfile"
	KeyValueEntryHintfileSuffix         = ".hfile"
	InactiveKeyValueEntryDataFileSuffix = ".idfile"
	SwapFilePattern                     = "*.swp"
	TempDataFilePattern                 = "*.dfile"
	TempInactiveDataFilePattern         = "*.idfile"
	DefaultDataDir                      = "nimbusdb"

	DatafileThreshold = 1 * MB
	BlockSize         = 32 * KB
)

const (
	CrcSize         int64 = 5
	DeleteFlagSize        = 1
	TstampSize            = 10
	KeySizeSize           = 10
	ValueSizeSize         = 10
	StaticChunkSize       = CrcSize + DeleteFlagSize + TstampSize + KeySizeSize + ValueSizeSize

	CrcOffset        int64 = 5
	DeleteFlagOffset       = 6
	TstampOffset           = 16
	KeySizeOffset          = 26
	ValueSizeOffset        = 36

	BTreeDegree int = 10
)

const (
	TotalStaticChunkSize int64 = TstampOffset + KeySizeOffset + ValueSizeOffset + DeleteFlagOffset + CrcOffset + StaticChunkSize
)

var (
	ERROR_KEY_NOT_FOUND             = errors.New("key expired or does not exist")
	ERROR_NO_ACTIVE_FILE_OPENED     = errors.New("no file opened for writing")
	ERROR_OFFSET_EXCEEDED_FILE_SIZE = errors.New("offset exceeded file size")
	ERROR_CANNOT_READ_FILE          = errors.New("error reading file")
	ERROR_KEY_VALUE_SIZE_EXCEEDED   = errors.New(fmt.Sprintf("exceeded limit of %d bytes", BlockSize))
	ERROR_CRC_DOES_NOT_MATCH        = errors.New("crc does not match. corrupted datafile")
	ERROR_DB_CLOSED                 = errors.New("database is closed")
)

var (
	ERROR_BATCH_CLOSED              = errors.New("batch is closed")
	ERROR_CANNOT_CLOSE_CLOSED_BATCH = errors.New("cannot close closed batch")
)

const (
	KEY_EXPIRES_IN_DEFAULT = 168 * time.Hour // 1 week

	DELETED_FLAG_BYTE_VALUE  = byte(0x31)
	DELETED_FLAG_SET_VALUE   = byte(0x01)
	DELETED_FLAG_UNSET_VALUE = byte(0x00)

	LRU_SIZE = 50
	LRU_TTL  = 24 * time.Hour

	EXIT_NOT_OK = 0
	EXIT_OK     = 1

	INITIAL_SEGMENT_OFFSET         = 0
	INITIAL_KEY_VALUE_ENTRY_OFFSET = 0
)

type Options struct {
	IsMerge              bool
	CurrentMergeFilePath string

	Path           string
	ShouldWatch    bool
	WatchQueueSize int
}

type KeyValuePair struct {
	Key   []byte
	Value interface{}
	Ttl   time.Duration
}

type KeyDirValue struct {
	offset      int64
	blockNumber int64
	size        int64
	path        string
	tstamp      int64
}

type ActiveKeyValueOffset struct {
	Startoffset int64
	Endoffset   int64
}

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

type Batch struct {
	id         ksuid.KSUID
	db         *Db
	closed     bool
	batchlock  sync.Mutex
	mu         sync.RWMutex
	writeQueue []*KeyValuePair
}

// BlockOffsetPair contains metadata about the Block. The start and ending offsets of the Block, and the path.
type BlockOffsetPair struct {
	startOffset int64
	endOffset   int64
	filePath    string
}
