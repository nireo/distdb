package engine

import (
	"bytes"
	"io/ioutil"
	"os"
	"testing"
)

func TestBadgerStorage(t *testing.T) {
	dir, err := ioutil.TempDir("", "storage-test")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)

	kv, err := NewKVStoreWithPath(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer kv.Close()

	key := []byte("hello")
	value := []byte("value")

	if err := kv.Put(key, value); err != nil {
		t.Fatal(err)
	}

	val, err := kv.Get(key)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(val, value) {
		t.Fatalf("byte values are not equal. got=%s want=%s", string(val), string(value))
	}

	if err := kv.Delete(key); err != nil {
		t.Fatal(err)
	}

	_, err = kv.Get(key)
	if err == nil {
		t.Fatalf("key was not deleted successfully")
	}
}
