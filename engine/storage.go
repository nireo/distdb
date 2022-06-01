package engine

import (
	"bytes"
	"errors"
	"log"
	"path/filepath"
	"sync"

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
	SnapshotItems() <-chan *api.Record
	GetUnderlying() *T
}

type KVPair struct {
	Key   []byte
	Value []byte
}

// implements the storage
type KVStore struct {
	db    *bolt.DB
	cache map[string][]byte // memory caching for raft.
	mu    sync.Mutex
}

// Put places a key into the database.
func (kv *KVStore) Put(key, value []byte) error {
	return kv.db.Update(func(tx *bolt.Tx) error {
		if err := tx.Bucket(defaultBucket).Put([]byte(key), value); err != nil {
			return err
		}

		return tx.Bucket(replicationBucket).Put([]byte(key), value)
	})

}

// Get finds a given key from the database or returns a error, if that
// key was not found.
func (kv *KVStore) Get(key []byte) ([]byte, error) {
	var res []byte
	err := kv.db.View(func(tx *bolt.Tx) error {
		res = copyByteSlice(tx.Bucket(defaultBucket).Get(key))
		return nil
	})

	if res == nil {
		return nil, api.ErrKeyNotFound{Key: key}
	}

	if err == nil {
		return res, nil
	}

	return nil, api.ErrKeyNotFound{Key: key}
}

// Delete removes a given key from the database.
func (kv *KVStore) Delete(key []byte) error {
	err := kv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		err := b.Delete(key)

		return err
	})

	return err
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
		b := tx.Bucket(defaultBucket)
		return b.ForEach(func(k, v []byte) error {
			pairs = append(pairs, &api.Record{
				Key:   copyByteSlice(k),
				Value: copyByteSlice(v),
			})
			return nil
		})
	})
	if err == nil {
		return pairs, nil
	}
	return nil, err
}

func (kv *KVStore) ScanWithPrefix(pref []byte) ([]*api.Record, error) {
	pairs := []*api.Record{}

	err := kv.db.View(func(tx *bolt.Tx) error {
		// Assume bucket exists and has keys
		b := tx.Bucket(defaultBucket)
		c := b.Cursor()

		for k, v := c.Seek(pref); k != nil && bytes.HasPrefix(k, pref); k, v = c.Next() {
			pairs = append(pairs, &api.Record{
				Key:   copyByteSlice(k),
				Value: copyByteSlice(v),
			})
		}

		return nil
	})

	if err == nil {
		return pairs, nil
	}
	return nil, err
}

func copyByteSlice(b []byte) []byte {
	if b == nil {
		return nil
	}
	res := make([]byte, len(b))
	copy(res, b)
	return res
}

func (kv *KVStore) GetNextReplicationKey() (*api.Record, error) {
	res := &api.Record{}

	err := kv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationBucket)
		k, v := b.Cursor().First()
		res.Key = copyByteSlice(k)
		res.Value = copyByteSlice(v)

		return nil
	})

	if err != nil {
		return nil, err
	}

	return res, nil
}

func (kv *KVStore) DeleteReplicationKey(rec *api.Record) error {
	return kv.db.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket(replicationBucket)

		v := b.Get(rec.Key)
		if v == nil {
			return errors.New("key doesn't exist")
		}

		if !bytes.Equal(v, rec.Value) {
			return errors.New("value does not match")
		}

		return b.Delete(rec.Key)
	})
}

func (kv *KVStore) PutWithoutReplication(key, value []byte) error {
	return kv.db.Update(func(tx *bolt.Tx) error {
		return tx.Bucket(defaultBucket).Put(key, value)
	})
}

func (kv *KVStore) GetSnapshotItems() <-chan *api.Record {
	ch := make(chan *api.Record, 1024)

	go kv.db.View(func(tx *bolt.Tx) error {
		b := tx.Bucket(defaultBucket)
		c := b.Cursor()

		for k, v := c.First(); k != nil; k, v = c.Next() {
			log.Printf("snapshotting %s:%s\n", string(k), string(v))
			ch <- &api.Record{
				Key:   append([]byte{}, k...),
				Value: append([]byte{}, v...),
			}

		}

		ch <- &api.Record{
			Key:   nil,
			Value: nil,
		}

		return nil
	})

	return ch
}
