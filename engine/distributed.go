package engine

import (
	"crypto/tls"
	"net"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft"
	api "github.com/nireo/distdb/api/v1"
	"google.golang.org/protobuf/proto"
)

type RequestType uint8

const (
	WriteRequestType RequestType = 0
)

type Config struct {
	Raft struct {
		raft.Config
		StreamLayer *StreamLayer
		Bootstrap   bool
	}
}

type DistDB struct {
	config Config
	db     *KVStore
	raft   *raft.Raft
}

type StreamLayer struct {
	ln              net.Listener
	serverTLSConfig *tls.Config
	peerTLSConfig   *tls.Config
}

type fsm struct {
	db *KVStore
}

var _ raft.FSM = (*fsm)(nil)

func (l *fsm) Apply(record *raft.Log) interface{} {
	buf := record.Data
	reqType := RequestType(buf[0])
	switch reqType {
	case WriteRequestType:
		return l.applyWrite(buf[1:])
	}
	return nil
}

func (l *fsm) applyWrite(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	err = l.db.Put(req.Record.Key, req.Record.Value)
	if err != nil {
		return err
	}
	return &api.ProduceResponse{}
}

func NewDistDB(dataDir string, config Config) (*DistDB, error) {
	l := &DistDB{
		config: config,
	}
	if err := l.setupLog(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return nil, nil
}

func (d *DistDB) setupLog(dataDir string) error {
	dbDir := filepath.Join(dataDir, "db")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return err
	}
	var err error
	d.db, err = NewKVStore(dbDir)
	return err
}

func (d *DistDB) setupRaft(dataDir string) error {
	return nil
}
