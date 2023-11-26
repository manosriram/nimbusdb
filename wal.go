package nimbusdb

import (
	"os"
	"time"

	"github.com/manosriram/nimbusdb/utils"
)

func (db *Db) WriteSegment(segment *Segment) error {
	f, err := os.OpenFile(db.activeDataFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	_, err = f.Write(segment.ToByte())
	if err != nil {
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}

	return nil
}

func (db *Db) ExpireKey(offset int64) error {
	f, err := os.OpenFile(db.activeDataFile, os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	expireTstamp := time.Now().Add(-1 * time.Hour).UnixNano()
	_, err = f.WriteAt(utils.Encode(expireTstamp), int64(offset))
	if err != nil {
		return err
	}

	return nil
}
