package engine

import (
	"bytes"
	"path/filepath"

	api "github.com/nireo/distdb/api/v1"
	bolt "go.etcd.io/bbolt"
)

var (
	defaultBucket     = []byte("default")
	replicationBucket = []byte("replication")
)

// Common storage interface interact with internal storage of a
// distributed node.
type Storage[T interface{}] interface {
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
	IterateKeysAndPairs() ([]*api.Record, error)
	ScanWithPrefix([]byte) ([]*api.Record, error)
	GetUnderlying() *T
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// implements the storage
type KVStore struct {
	db *bolt.DB
}

// Put places a key into the database.
func (kv *KVStore) Put(key, value []byte) error {
	err := kv.db.Update(func(tx *bolt.Tx) error {
		tx.Bucket(defaultBucket).Put(key, value)
		return nil
	})
	return err
}

// Get finds a given key from the database or returns a error, if that
// key was not found.
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	var res []byte
	err := kv.db.View(func(tx *bolt.Tx) error {
		res = copyByteSlice(tx.Bucket(defaultBucket).Get(key))
		return nil
	})

	if err == nil {
		return res, nil
	}

	return nil, api.ErrKeyNotFound{Key: key}
}

// Delete removes a given key from the database.
func (kv *KVStore) Delete(key []byte) error {
	err := kv.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Delete(key); err != nil {
			return err
		}

		return nil
	})

	if err != nil {
		return err
	}
	return nil
}

// NewKVStore creates a badger.DB instance with generally good settings.
func NewKVStore(path string) (*KVStore, error) {
	// We don't want a logger clogging up test screens.
	db, err := bolt.Open(filepath.Join(path, "storage.db"), 0600, nil)
	if err != nil {
		return nil, err
	}

	if err := db.Update(func(tx *bolt.Tx) error {
		if _, err := tx.CreateBucketIfNotExists(defaultBucket); err != nil {
			return err
		}

		if _, err := tx.CreateBucketIfNotExists(replicationBucket); err != nil {
			return err
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return &KVStore{db: db}, nil
}

// Close closes the connection to the badger database. Since we don't
// want to defer closing in the creation function, we need to create a
// custom close method here.
func (kv *KVStore) Close() error {
	return kv.db.Close()
}

func (kv *KVStore) GetUnderlying() *bolt.DB {
	return kv.db
}

func (kv *KVStore) IterateKeysAndPairs() ([]*api.Record, error) {
	pairs := []*api.Record{}

	err := kv.db.View(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).ForEach(func(k, v []byte) error {
			pairs = append(pairs, &api.Record{
				Key:   copyByteSlice(k),
				Value: copyByteSlice(v),
			})
			return nil
		})
	})
	if err != nil {
		return nil, err
	}

	return pairs, nil
}

func (kv *KVStore) ScanWithPrefix(pref []byte) ([]*api.Record, error) {
	pairs := []*api.Record{}

	err := kv.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		c := tx.Bucket([]byte("MyBucket")).Cursor()

		for k, v := c.Seek(pref); k != nil && bytes.HasPrefix(k, pref); k, v = c.Next() {
			pairs = append(pairs, &api.Record{
				Key:   copyByteSlice(k),
				Value: copyByteSlice(v),
			})
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return pairs, nil
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}
