package engine

import (
	"encoding/binary"
	"fmt"
	rbt "github.com/emirpasic/gods/trees/redblacktree"
	"os"
	"sync"
)

const MaxMemtableSize uint64 = 4 * 1024 * 1024 // 4 mb

type Memtable struct {
	keys      *rbt.Tree
	keyMutex  sync.RWMutex
	size      uint64
	logFile   *os.File
	fileMutex sync.Mutex
}

func (mem *Memtable) Insert(key, value string) error {
	if err := mem.AppendToLog(key, value); err != nil {
		return err
	}

	mem.keyMutex.Lock()
	mem.size += uint64(len(key) + len(value))
	mem.keys.Put(key, value)

	mem.keyMutex.Unlock()

	return nil
}

func (mem *Memtable) AppendToLog(key, value string) error {
	mem.fileMutex.Lock()
	defer mem.fileMutex.Unlock()

	keyBytes := []byte(key)
	valueBytes := []byte(value)

	logWrite := make([]byte, 6)
	binary.LittleEndian.PutUint16(logWrite[0:1], uint16(len(keyBytes)))
	binary.LittleEndian.PutUint32(logWrite[2:5], uint32(len(valueBytes)))

	logWrite = append(logWrite, keyBytes...)
	logWrite = append(logWrite, valueBytes...)

	nbytes, err := mem.logFile.Write(logWrite)
	if err != nil {
		return err
	}

	logSize := len(logWrite)
	if nbytes != logSize {
		return fmt.Errorf("wrong amount of bytes written to log. got=%d wanted=%d", nbytes, logSize)
	}

	return nil

}

func (mem *Memtable) Get(key string) (string, error) {
	mem.keyMutex.RLock()
	defer mem.keyMutex.RUnlock()

	if val, ok := mem.keys.Get(key); !ok {
		return "", fmt.Errorf("couldn't find key '%s'", key)
	} else {
		return val.(string), nil
	}
}

func NewMemtable() *Memtable {
	return &Memtable{
		size:      0,
		keys:      rbt.NewWithStringComparator(),
		keyMutex:  sync.RWMutex{},
		logFile:   nil,
		fileMutex: sync.Mutex{},
	}
}
