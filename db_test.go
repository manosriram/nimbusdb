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

var opts = &Options{
	Path:        utils.DbDir(),
	ShouldWatch: false,
}

const (
	EXPIRY_DURATION = 1 * time.Second
)

func TestDbOpen(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)
}

func Test_InMemory_SetGet_With_TTL(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	v, err := d.SetWithTTL([]byte("testkey1"), []byte("testvalue1"), EXPIRY_DURATION)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	time.Sleep(2 * time.Second)
	va, err := d.Get([]byte("testkey1"))
	assert.NotEqual(t, nil, err)
	assert.NotEqual(t, []byte("testvalue1"), va)

	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_InMemory_SetGet(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testkey1")
	value := []byte("testvalue1")
	v, err := d.Set(key, value)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	va, err := d.Get(key)
	assert.Equal(t, nil, err)
	assert.Equal(t, value, va)
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_InMemory_Stress_SetGet(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte("testkey")

		_, err := d.Set(key, value)
		assert.Nil(t, err)
	}

	for i := 0; i < 100000; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte("testkey")
		va, err := d.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value, va)
	}
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_Set(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testkey")
	value := []byte("testvalue")
	_, err = d.Set(key, value)
	assert.Nil(t, err)
}

func Test_Get(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testkey")
	value := []byte("testvalue")
	va, err := d.Get(key)
	assert.Nil(t, err)
	assert.Equal(t, value, va)
}

func Test_Delete(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testkey")
	// value := []byte("testvalue")
	err = d.Delete(key)
	assert.Equal(t, nil, err)

	_, err = d.Get(key)
	assert.Equal(t, err, ERROR_KEY_NOT_FOUND)
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_InMemory_Delete(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testkey1")
	value := []byte("testvalue1")
	v, err := d.Set(key, value)
	assert.Equal(t, err, nil)
	assert.Equal(t, v, []byte("testvalue1"))

	va, err := d.Get(key)
	assert.Equal(t, nil, err)
	assert.Equal(t, value, va)

	err = d.Delete(key)
	assert.Equal(t, nil, err)

	va, err = d.Get(key)
	assert.Equal(t, err, ERROR_KEY_NOT_FOUND)
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_StressSet(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte("testvalue")
		_, err := d.Set(key, value)
		assert.Nil(t, err)
	}
}

func Test_StressGet(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	for i := 0; i < 100000; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte("testvalue")
		v, err := d.Get(key)
		assert.Nil(t, err)
		assert.Equal(t, value, v)
	}
	t.Cleanup(func() {
		os.RemoveAll(opts.Path)
	})
}

func Test_ConcurrentSet(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		key := []byte(utils.GetTestKey(i))
		value := []byte(fmt.Sprintf("testvalue%d", i))
		go func() {
			defer wg.Done()
			_, err := d.Set(key, value)
			assert.Nil(t, err)
		}()
	}
	wg.Wait()
}

func Test_ConcurrentGet(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		kv := &KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			v, err := d.Get(kv.Key)
			assert.Nil(t, err)
			assert.Equal(t, kv.Value, v)
		}()
	}
	wg.Wait()
}

func Test_ConcurrentDelete(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 10000

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	for i := 0; i < numGoRoutines; i++ {
		kv := &KeyValuePair{
			Key:   []byte(utils.GetTestKey(i)),
			Value: []byte(fmt.Sprintf("testvalue%d", i)),
		}
		go func() {
			defer wg.Done()
			err := d.Delete(kv.Key)
			assert.Nil(t, err)

			_, err = d.Get(kv.Key)
			assert.Equal(t, ERROR_KEY_NOT_FOUND, err)
		}()
	}
	wg.Wait()
}
