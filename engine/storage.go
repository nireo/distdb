package engine

import (
	badger "github.com/dgraph-io/badger/v3"
	api "github.com/nireo/distdb/api/v1"
)

// Common storage interface interact with internal storage of a
// distributed node.
type Storage[T interface{}] interface {
	Put([]byte, []byte) error
	Get([]byte) ([]byte, error)
	Delete([]byte) error
	GetUnderlying() *T
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
	// We don't want a logger clogging up test screens.
	opts := badger.DefaultOptions("./")
	opts.Logger = nil

	db, err := badger.Open(opts)
	if err != nil {
		return nil, err
	}
	return &KVStore{db: db}, nil
}

// NewKVStoreWithPath creates a badge.DB instance with the same options as
// NewKVStore, but it also includes a given path.
func NewKVStoreWithPath(path string) (*KVStore, error) {
	// We don't want a logger clogging up test screens.
	opts := badger.DefaultOptions(path)
	opts.Logger = nil

	db, err := badger.Open(opts)
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

func (kv *KVStore) GetUnderlying() *badger.DB {
	return kv.db
}

func (kv *KVStore) WriteBatch(pairs []*api.Record) error {
	batch := kv.db.NewWriteBatch()
	defer batch.Cancel()

	for idx := range pairs {
		entry := badger.NewEntry(pairs[idx].Key, pairs[idx].Value)
		if err := batch.SetEntry(entry); err != nil {
			return err
		}
	}

	return batch.Flush()
}

func (kv *KVStore) GetBatch(keys [][]byte) ([]*api.Record, error) {
	pairs := []*api.Record{}

	for idx := range keys {
		val, err := kv.Get(keys[idx])
		if err != nil {
			continue
		}

		if val == nil {
			continue
		}

		pairs = append(pairs, &api.Record{
			Key:   keys[idx],
			Value: val,
		})
	}

	return pairs, nil
}

func (kv *KVStore) IterateKeysAndPairs() ([]*api.Record, error) {
	pairs := []*api.Record{}

	err := kv.db.View(func(txn *badger.Txn) error {
		opts := badger.DefaultIteratorOptions
		opts.PrefetchSize = 10
		it := txn.NewIterator(opts)
		defer it.Close()

		for it.Rewind(); it.Valid(); it.Next() {
			item := it.Item()
			k := item.Key()
			if err := item.Value(func(v []byte) error {
				pairs = append(pairs, &api.Record{
					Key:   k,
					Value: v,
				})
				return nil
			}); err != nil {
				return err
			}
		}

		return nil
	})

	if err != nil {
		return nil, err
	}

	return pairs, nil
}

func (kv *KVStore) ScanWithPrefix(pref []byte) ([]*api.Record, error) {
	pairs := []*api.Record{}

	err := kv.db.View(func(txn *badger.Txn) error {
		it := txn.NewIterator(badger.DefaultIteratorOptions)
		defer it.Close()

		for it.Seek(pref); it.ValidForPrefix(pref); it.Next() {
			item := it.Item()
			k := it.Item()

			if err := item.Value(func(v []byte) error {
				pairs = append(pairs, &api.Record{
					Key:   k.Key(),
					Value: v,
				})
				return nil
			}); err != nil {
				return nil
			}
		}
		return nil
	})

	if err != nil {
		return nil, err
	}

	return pairs, nil
}
