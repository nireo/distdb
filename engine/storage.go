package engine

import (
	badger "github.com/dgraph-io/badger/v3"
)

type Storage interface {
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
}

// implements the storage
type KVStore struct {
	db *badger.DB
}

func (kv *KVStore) Put(key, value []byte) error {
	return kv.db.Update(func(txn *badger.Txn) error {
		return txn.Set(key, value)
	})
}

func (kv *KVStore) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := kv.db.View(func(txn *badger.Txn) error {
		item, err := txn.Get(key)
		if err != nil {
			return err
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

func (kv *KVStore) Delete(key []byte) error {
	return kv.db.Update(func(txn *badger.Txn) error {
		return txn.Delete(key)
	})
}

func NewKVStore() (*KVStore, error) {
	db, err := badger.Open(badger.DefaultOptions("./"))
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}

func NewKVStoreWithPath(path string) (*KVStore, error) {
	db, err := badger.Open(badger.DefaultOptions(path))
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}

func (kv *KVStore) Close() {
	kv.db.Close()
}
