package engine

import (
	badger "github.com/dgraph-io/badger/v3"
	api "github.com/nireo/distdb/api/v1"
)

// Common storage interface interact with internal storage of a
// distributed node.
type Storage interface {
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// implements the storage
type KVStore struct {
	db *badger.DB
}

// Put places a key into the database.
func (kv *KVStore) Put(key, value []byte) error {
	return kv.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

// Get finds a given key from the database or returns a error, if that
// key was not found.
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return api.ErrKeyNotFound{Key: key}
		}

		valCopy, err = item.ValueCopy(nil)
		if err != nil {
			return err
		}

		return nil
	})
	if err != nil {
		return nil, err
	}

	return valCopy, nil
}

// Delete removes a given key from the database.
func (kv *KVStore) Delete(key []byte) error {
	return kv.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

// NewKVStore creates a badger.DB instance with generally good settings.
func NewKVStore() (*KVStore, error) {
	db, err := badger.Open(badger.DefaultOptions("./"))
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}

// NewKVStoreWithPath creates a badge.DB instance with the same options as
// NewKVStore, but it also includes a given path.
func NewKVStoreWithPath(path string) (*KVStore, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}

// Close closes the connection to the badger database. Since we don't
// want to defer closing in the creation function, we need to create a
// custom close method here.
func (kv *KVStore) Close() {
	kv.db.Close()
}
