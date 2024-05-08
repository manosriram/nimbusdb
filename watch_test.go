package nimbusdb

import (
	"os"
	"testing"

	"github.com/manosriram/nimbusdb/utils"
	"github.com/stretchr/testify/assert"
)

func Test_Watch_Set(t *testing.T) {
	d, err := Open(&Options{
		Path:           utils.DbDir(),
		WatchQueueSize: 10,
		ShouldWatch:    true,
	})
	defer d.Close()
	defer d.CloseWatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	ch, err := d.NewWatch()
	assert.Nil(t, err)

	k := []byte("testkey1")
	v := []byte("testvalue1")
	_, err = d.Set(k, v)
	assert.Nil(t, err)

	event := <-ch
	assert.Equal(t, Create, event.EventType)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Watch_Update(t *testing.T) {
	d, err := Open(&Options{
		Path:           utils.DbDir(),
		WatchQueueSize: 10,
		ShouldWatch:    true,
	})
	defer d.Close()
	defer d.CloseWatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	ch, err := d.NewWatch()
	assert.Nil(t, err)

	k := []byte("testkey1")
	v := []byte("testvalue1")
	_, err = d.Set(k, v)
	_, err = d.Set(k, v)
	assert.Nil(t, err)

	createEvent := <-ch
	assert.Equal(t, Create, createEvent.EventType)

	updateEvent := <-ch
	assert.Equal(t, Update, updateEvent.EventType)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Watch_Delete(t *testing.T) {
	d, err := Open(&Options{
		Path:           utils.DbDir(),
		WatchQueueSize: 10,
		ShouldWatch:    true,
	})
	defer d.Close()
	defer d.CloseWatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	ch, err := d.NewWatch()
	assert.Nil(t, err)

	k := []byte("testkey1")
	v := []byte("testvalue1")
	_, err = d.Set(k, v)
	assert.Nil(t, err)

	_, err = d.Delete(k)
	assert.Nil(t, err)

	createEvent := <-ch
	assert.Equal(t, Create, createEvent.EventType)

	deleteEvent := <-ch
	assert.Equal(t, Delete, deleteEvent.EventType)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}
