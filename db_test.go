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
	// Path:        utils.DbDir(),
	Path:        "/Users/manosriram/nimbusdb/test_data",
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

func Test_Get_With_Revision(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testrevisionkey")
	firstValue := []byte("testvalue")
	updatedValue := []byte("updatedvalue")

	_, err = d.Set(key, firstValue)
	assert.Nil(t, err)

	_, err = d.Set(key, updatedValue)
	assert.Nil(t, err)

	v1, err := d.GetUsingRevision(key, 1)
	assert.Nil(t, err)

	v2, err := d.GetUsingRevision(key, 2)
	assert.Nil(t, err)

	assert.Equal(t, v1, firstValue)
	assert.Equal(t, v2, updatedValue)
}

func Test_Delete(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	key := []byte("testkey")
	// value := []byte("testvalue")
	_, err = d.Delete(key)
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

	_, err = d.Delete(key)
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

func Test_StressGetRevision(t *testing.T) {
	d, err := Open(opts)
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	defer d.Close()
	var values []string

	for i := 0; i < 5; i++ {
		key := []byte("getrevision-test")
		value := []byte("testvalue")

		_, err := d.Set(key, value)
		assert.Nil(t, err)

		values = append(values, string(value))
	}

	for i := 0; i < 5; i++ {
		key := []byte("getrevision-test")
		v, err := d.GetUsingRevision(key, int64(i+1))
		assert.Nil(t, err)
		assert.Equal(t, values[i], string(v))
	}
	// t.Cleanup(func() {
	// os.RemoveAll(opts.Path)
	// })
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

func Test_ConcurrentGetRevision(t *testing.T) {
	d, err := Open(opts)
	defer d.Close()
	assert.Equal(t, err, nil)
	assert.NotEqual(t, d, nil)

	numGoRoutines := 25

	wg := sync.WaitGroup{}
	wg.Add(numGoRoutines)

	var revisionValues []string
	for i := 0; i < numGoRoutines; i++ {
		v := []byte(fmt.Sprintf("testrevisionvalue-%d", i))
		_, err := d.Set([]byte("testrevision-c"), v)

		revisionValues = append(revisionValues, string(v))
		assert.Nil(t, err)
	}

	for i := 0; i < numGoRoutines; i++ {
		kv := &KeyValuePair{
			Key: []byte("testrevision-c"),
		}
		go func(I int) {
			defer wg.Done()
			vv, err := d.GetUsingRevision(kv.Key, int64(I+1))
			fmt.Println(vv, string(vv))
			assert.Nil(t, err)
			assert.Equal(t, revisionValues[I], string(vv))
		}(i)
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
			_, err := d.Delete(kv.Key)
			assert.Nil(t, err)

			_, err = d.Get(kv.Key)
			assert.Equal(t, ERROR_KEY_NOT_FOUND, err)
		}()
	}
	wg.Wait()
}
