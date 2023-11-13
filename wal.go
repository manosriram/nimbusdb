package nimbusdb

import (
	"os"
)

func (db *Db) Write(segment *Segment) error {
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
