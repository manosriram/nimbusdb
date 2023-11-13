package nimbusdb

import (
	"os"
)

func (db *Db) Write(segment *Segment) error {
	f, err := os.OpenFile(db.activeDataFile, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return err
	}

	f.Write(segment.tstamp)
	f.Write(segment.ksz)
	f.Write(segment.vsz)
	f.Write(segment.k)
	f.Write(segment.v)

	if err := f.Close(); err != nil {
		return err
	}

	return nil
}
