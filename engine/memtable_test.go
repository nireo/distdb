package engine

import (
	"errors"
	"os"
	"testing"
)

func TestBasicOperations(t *testing.T) {
	key := "hello"
	value := "world"

	mem := NewMemtable()
	err := mem.Insert(key, value)
	if err != nil {
		t.Error(err)
	}

	val, err := mem.Get(key)
	if err != nil {
		t.Error(err)
	}

	if val != value {
		t.Errorf("values are not equal, got=%s wanted=%s", value, val)
	}
}

func LogFileCreated(t *testing.T) {
	mem := NewMemtable()

	if _, err := os.Stat("/path/to/whatever"); errors.Is(err, os.ErrNotExist) {
		t.Error(err)
	}
}
