package nimbusdb

import (
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/manosriram/nimbusdb/utils"
	"github.com/stretchr/testify/assert"
)

func Test_Batch_SetGet(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer d.Close()
	defer b.Close()

	k := []byte("testkey1")
	v := []byte("testvalue1")
	val, err := b.Set(k, v)
	assert.Equal(t, err, nil)
	val, err = b.Get(k)
	assert.Equal(t, err, nil)
	assert.Equal(t, val, v)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Batch_InMemory_SetGet_With_TTL(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer d.Close()
	defer b.Close()

	v, err := b.SetWithTTL([]byte("testkey1"), []byte("testvalue1"), EXPIRY_DURATION)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	b.Commit()
	time.Sleep(2 * time.Second)
	va, err := b.Get([]byte("testkey1"))
	assert.NotEqual(t, nil, err)
	assert.NotEqual(t, []byte("testvalue1"), va)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Batch_InMemory_Stress_SetGet(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer b.Close()
	defer d.Close()

	for i := 0; i < 10000; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte("testkey")

		_, err := b.Set(key, value)
		assert.Nil(t, err)
	}
	b.Commit()

	for i := 0; i < 10000; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte("testkey")
		va, err := b.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value, va)
	}
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Batch_Commit(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer b.Close()
	defer d.Close()

	key := []byte("testkey")
	value := []byte("testvalue")

	_, err = b.Set(key, value)
	assert.Nil(t, err)

	b.Commit()
	b2, err := d.NewBatch()
	assert.Equal(t, err, nil)

	va, err := b2.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, va)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Batch_Rollback(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer b.Close()
	defer d.Close()

	key := []byte("testkey")
	value := []byte("testvalue")

	_, err = b.Set(key, value)
	assert.Nil(t, err)

	b.Rollback()
	b2, err := d.NewBatch()
	assert.Equal(t, err, nil)

	_, err = b2.Get(key)
	assert.NotNil(t, err)
	assert.Equal(t, ERROR_KEY_NOT_FOUND, err)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Batch_Delete(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer b.Close()
	defer d.Close()

	key := []byte("testkey")
	value := []byte("testvalue")

	_, err = b.Set(key, value)
	assert.Nil(t, err)

	va, err := b.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, va)

	_, err = b.Delete(key)
	assert.Nil(t, err)

	_, err = d.Get(key)
	assert.Equal(t, err, ERROR_KEY_NOT_FOUND)
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Batch_ConcurrentSet(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer d.Close()
	defer b.Close()

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		go func() {
			defer wg.Done()
			_, err := b.Set(key, value)
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

func Test_Batch_ConcurrentGet(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer b.Close()
	defer d.Close()

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		go func() {
			defer wg.Done()
			v, err := b.Get(key)
			assert.Nil(t, err)
			assert.Equal(t, value, v)
		}()
	}
	wg.Wait()
}

func Test_Batch_ConcurrentDelete(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	b, err := d.NewBatch()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, b, nil)

	defer d.Close()
	defer b.Close()

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		key := []byte(utils.GetTestKey(i))
		go func() {
			defer wg.Done()
			_, err := b.Delete(key)
			assert.Nil(t, err)

			_, err = b.Get(key)
			assert.Equal(t, ERROR_KEY_NOT_FOUND, err)
		}()
	}
	wg.Wait()
}
