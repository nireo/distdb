package engine

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
	"time"

	api "github.com/nireo/distdb/api/v1"
	"github.com/stretchr/testify/require"
)

func TestBadgerStorage(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	kv, err := NewKVStore(dir)
	require.NoError(t, err)
	defer kv.Close()

	key := []byte("hello")
	value := []byte("value")

	err = kv.Put(key, value)
	require.NoError(t, err)

	val, err := kv.Get(key)
	require.NoError(t, err)

	if !bytes.Equal(val, value) {
		t.Fatalf("byte values are not equal. got=%s want=%s", string(val), string(value))
	}

	time.Sleep(time.Second)

	err = kv.Delete([]byte("hello"))
	require.NoError(t, err)

	_, err = kv.Get([]byte("hello"))
	if err == nil {
		t.Fatalf("key was not deleted successfully")
	}
}

func TestKeyNotFound(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	kv, err := NewKVStore(dir)
	require.NoError(t, err)
	defer kv.Close()

	val, err := kv.Get([]byte("nonexistant"))
	if val != nil {
		t.Fatalf("key value should be nil as it doesn't exist")
	}

	apiErr := err.(api.ErrKeyNotFound)
	if !bytes.Equal(apiErr.Key, []byte("nonexistant")) {
		t.Fatalf("error message key is not same as requested key")
	}
}

func TestGetAll(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	kv, err := NewKVStore(dir)
	require.NoError(t, err)
	defer kv.Close()

	err = kv.Put([]byte("hello"), []byte("world"))
	require.NoError(t, err)

	err = kv.Put([]byte("world"), []byte("hello"))
	require.NoError(t, err)

	all, err := kv.IterateKeysAndPairs()
	require.NoError(t, err)

	require.Equal(t, 2, len(all))
}

func TestPrefixScan(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage-test")
	require.NoError(t, err)
	defer os.RemoveAll(dir)

	kv, err := NewKVStore(dir)
	require.NoError(t, err)
	defer kv.Close()

	err = kv.Put([]byte("hello"), []byte("world"))
	require.NoError(t, err)

	err = kv.Put([]byte("world"), []byte("hello"))
	require.NoError(t, err)

	prefixScanned, err := kv.ScanWithPrefix([]byte("hel"))
	require.NoError(t, err)

	require.Equal(t, 1, len(prefixScanned))
}
