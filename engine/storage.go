package engine

import (
	badger "github.com/dgraph-io/badger/v3"
	api "github.com/nireo/distdb/api/v1"
	"github.com/xujiajun/nutsdb"
)

const (
	defaultBucket     = "default"
	replicationBucket = "replication"
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
	db *nutsdb.DB
}

// Put places a key into the database.
func (kv *KVStore) Put(key, value []byte) error {
	return kv.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Put(defaultBucket, key, value, 0)
	})
}

// Get finds a given key from the database or returns a error, if that
// key was not found.
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	var valCopy []byte
	err := kv.db.View(func(tx *nutsdb.Tx) error {
		if e, err := tx.Get(defaultBucket, key); err != nil {
			return err
		} else {
			valCopy = append(valCopy, e.Value...)
			return nil
		}
	})
	if err != nil {
		return nil, err
	}

	return valCopy, nil
}

// Delete removes a given key from the database.
func (kv *KVStore) Delete(key []byte) error {
	return kv.db.Update(func(tx *nutsdb.Tx) error {
		return tx.Delete(defaultBucket, key)
	})
}

// NewKVStore creates a badger.DB instance with generally good settings.
func NewKVStore(path string) (*KVStore, error) {
	// We don't want a logger clogging up test screens.
	opts := nutsdb.DefaultOptions
	opts.Dir = path

	db, err := nutsdb.Open(opts)
	if err != nil {
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

func (kv *KVStore) GetUnderlying() *nutsdb.DB {
	return kv.db
}

func (kv *KVStore) IterateKeysAndPairs() ([]*api.Record, error) {
	pairs := []*api.Record{}

	if err := kv.db.View(func(tx *nutsdb.Tx) error {
		entries, err := tx.GetAll(defaultBucket)
		if err != nil {
			return err
		}

		for _, entry := range entries {
			pairs = append(pairs, &api.Record{
				Key:   entry.Key,
				Value: entry.Value,
			})
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return pairs, nil
}

func (kv *KVStore) ScanWithPrefix(pref []byte) ([]*api.Record, error) {
	pairs := []*api.Record{}

	if err := kv.db.View(func(tx *nutsdb.Tx) error {
		entryLimit := 50
		if entries, _, err := tx.PrefixScan(defaultBucket, pref, 0, entryLimit); err != nil {
			return err
		} else {
			for _, entry := range entries {
				pairs = append(pairs, &api.Record{
					Key:   entry.Key,
					Value: entry.Value,
				})
			}
		}

		return nil
	}); err != nil {
		return nil, err
	}

	return pairs, nil
}
