package engine

import (
	"bytes"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb"
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

const RaftRPC = 1

func NewStreamLayer(ln net.Listener, serverTLSConfig,
	peerTLSConfig *tls.Config) *StreamLayer {
	return &StreamLayer{
		ln:              ln,
		serverTLSConfig: serverTLSConfig,
		peerTLSConfig:   peerTLSConfig,
	}
}

func (s *StreamLayer) Dial(addr raft.ServerAddress, timeout time.Duration) (
	net.Conn, error,
) {
	dialer := &net.Dialer{Timeout: timeout}
	var conn, err = dialer.Dial("tcp", string(addr))
	if err != nil {
		return nil, err
	}

	_, err = conn.Write([]byte{byte(RaftRPC)})
	if err != nil {
		return nil, err
	}

	if s.peerTLSConfig != nil {
		conn = tls.Client(conn, s.peerTLSConfig)
	}
	return conn, err
}

func (s *StreamLayer) Accept() (net.Conn, error) {
	conn, err := s.ln.Accept()
	if err != nil {
		return nil, err
	}

	b := make([]byte, 1)
	if _, err := conn.Read(b); err != nil {
		return nil, err
	}

	if bytes.Compare([]byte{(byte(RaftRPC))}, b) != 0 {
		return nil, fmt.Errorf("not a raft rpc")
	}

	if s.serverTLSConfig != nil {
		return tls.Server(conn, s.serverTLSConfig), nil
	}

	return conn, nil
}

func (s *StreamLayer) Close() error {
	return s.ln.Close()
}

func (s *StreamLayer) Addr() net.Addr {
	return s.ln.Addr()
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

func (d *DistDB) Close() error {
	f := d.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	return d.db.Close()
}

func (d *DistDB) GetServers() ([]*api.Server, error) {
	future := d.raft.GetConfiguration()
	if err := future.Error(); err != nil {
		return nil, err
	}

	var servers []*api.Server
	for _, server := range future.Configuration().Servers {
		servers = append(servers, &api.Server{
			Id:       string(server.ID),
			RpcAddr:  string(server.Address),
			IsLeader: d.raft.Leader() == server.Address,
		})
	}

	return servers, nil
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

func (f *fsm) Restore(r io.ReadCloser) error {
	b := make([]byte, 8)
	var buf bytes.Buffer

	for i := 0; ; i++ {
		_, err := io.ReadFull(r, b)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		size := int64(binary.LittleEndian.Uint64(b))
		if _, err := io.CopyN(&buf, r, size); err != nil {
			return err
		}

		record := &api.Record{}
		if err = proto.Unmarshal(buf.Bytes(), record); err != nil {
			return err
		}

		if err = f.db.Put(record.Key, record.Value); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

type snapshot struct {
	state map[string][]byte
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	// copy the memory cache
	f.db.mu.Lock()
	defer f.db.mu.Unlock()

	state := make(map[string][]byte)
	for k, v := range f.db.cache {
		state[k] = v
	}
	return &snapshot{state: state}, nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	err := func() error {
		var req api.MultipleConsume
		req.Pairs = []*api.Record{}

		for k, v := range s.state {
			req.Pairs = append(req.Pairs, &api.Record{
				Key:   []byte(k),
				Value: v,
			})
		}

		data, err := proto.Marshal(&req)
		if err != nil {
			return err
		}

		if _, err := sink.Write(data); err != nil {
			return err
		}

		return sink.Close()
	}()

	if err != nil {
		sink.Cancel()
	}
	return err
}

func (s *snapshot) Release() {}

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

	return l, nil
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
	fsm := &fsm{
		db: d.db,
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "stable"))
	if err != nil {
		return err
	}

	retain := 1
	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(dataDir, "raft"), retain, os.Stderr)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second
	transport := raft.NewNetworkTransport(
		d.config.Raft.StreamLayer,
		maxPool,
		timeout,
		os.Stderr,
	)

	config := raft.DefaultConfig()
	config.LocalID = d.config.Raft.LocalID
	if d.config.Raft.HeartbeatTimeout != 0 {
		config.HeartbeatTimeout = d.config.Raft.HeartbeatTimeout
	}

	if d.config.Raft.ElectionTimeout != 0 {
		config.ElectionTimeout = d.config.Raft.ElectionTimeout
	}

	if d.config.Raft.LeaderLeaseTimeout != 0 {
		config.LeaderLeaseTimeout = d.config.Raft.LeaderLeaseTimeout
	}

	if d.config.Raft.CommitTimeout != 0 {
		config.CommitTimeout = d.config.Raft.CommitTimeout
	}

	d.raft, err = raft.NewRaft(config, fsm, stableStore, stableStore, snapshotStore, transport)
	if err != nil {
		return err
	}

	hasState, err := raft.HasExistingState(stableStore, stableStore, snapshotStore)
	if err != nil {
		return err
	}

	if d.config.Raft.Bootstrap && !hasState {
		config := raft.Configuration{
			Servers: []raft.Server{{
				ID:      config.LocalID,
				Address: transport.LocalAddr(),
			}},
		}
		err = d.raft.BootstrapCluster(config).Error()
	}
	return err
}
