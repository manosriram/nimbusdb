package nimbusdb

import (
	"errors"
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/hashicorp/golang-lru/v2/expirable"
	"github.com/segmentio/ksuid"
	"go.uber.org/zap"
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
	KeysizeSize           = 10
	RevisionSize          = 10
	ValuesizeSize         = 10
	StaticChunkSize       = CrcSize + DeleteFlagSize + TstampSize + KeysizeSize + RevisionSize + ValuesizeSize

	CrcOffset        int64 = 5
	DeleteFlagOffset       = 6
	TstampOffset           = 16
	RevisionOffset         = 26
	KeySizeOffset          = 36
	ValueSizeOffset        = 46

	BTreeDegree int = 10
)

const (
	TotalStaticChunkSize int64 = TstampOffset + KeySizeOffset + RevisionOffset + ValueSizeOffset + DeleteFlagOffset + CrcOffset + StaticChunkSize
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
	IsMerge        bool
	MergeFilePath  string
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
	revision    int64
}

func NewKeyDirValue(offset, size, tstamp int64, path string, revision int64) *KeyDirValue {
	return &KeyDirValue{
		offset:   offset,
		size:     size,
		tstamp:   tstamp,
		path:     path,
		revision: revision,
	}
}

type Db struct {
	dbDirPath             string
	closed                bool
	dataFilePath          string
	activeDataFile        string
	activeDataFilePointer *os.File
	store                 *BTree
	opts                  *Options
	lastOffset            atomic.Int64
	mu                    sync.RWMutex
	segments              map[string]*Segment
	lru                   *expirable.LRU[int64, *Block]
	watcher               chan WatcherEvent
	lo                    *zap.SugaredLogger
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
	filepointer        *os.File
}

// BlockOffsetPair contains metadata about the Block. The start and ending offsets of the Block, and the path.
type BlockOffsetPair struct {
	startOffset int64
	endOffset   int64
	filePath    string
}

type Batch struct {
	id         ksuid.KSUID
	db         *Db
	closed     bool
	batchlock  sync.Mutex
	mu         sync.RWMutex
	writeQueue []*KeyValuePair
}
