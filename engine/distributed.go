package engine

import (
	"bytes"
	"encoding/binary"
	"errors"
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

var (
	ErrNotLeader = errors.New("current node is not leader")
)

type Config struct {
	Raft struct {
		raft.Config
		Bootstrap bool
	}
}

type DistDB struct {
	config Config
	fsm    *fsm
	raft   *raft.Raft
}

type fsm struct {
	db *KVStore
}

type getResponse struct {
	data []byte
	err  error
}

type putResponse struct {
	err error
}

func newFsm(path string) (*fsm, error) {
	db, err := NewKVStore(path)
	if err != nil {
		return nil, err
	}
	return &fsm{db: db}, nil
}

func (f *fsm) Close() error {
	return f.db.Close()
}

var _ raft.FSM = (*fsm)(nil)

func (l *fsm) Apply(record *raft.Log) interface{} {
	var action api.Action

	if err := proto.Unmarshal(record.Data, &action); err != nil {
		return err
	}

	switch action.Type {
	case api.Action_STORE_GET:
		var req api.ConsumeRequest
		if err := proto.Unmarshal(action.Submessage, &req); err != nil {
			return err
		}
		val, err := l.db.Get(req.Key)
		if err != nil {
			return err
		}

		return &getResponse{data: val, err: err}
	case api.Action_STORE_POST:
		var req api.ProduceRequest
		err := proto.Unmarshal(action.Submessage, &req)
		return &putResponse{err: err}
	default:
		panic("unrecognized type")
	}
}

func (d *DistDB) Close() error {
	f := d.raft.Shutdown()
	if err := f.Error(); err != nil {
		return err
	}

	return d.fsm.Close()
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

func (f *fsm) applyWrite(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}

	err = f.db.Put(req.Record)
	if err != nil {
		return err
	}

	return &api.ProduceResponse{}
}

func (f *fsm) applyDelete(b []byte) interface{} {
	var req api.ProduceRequest
	err := proto.Unmarshal(b, &req)

	return err
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

		if err = f.db.Put(record); err != nil {
			return err
		}

		buf.Reset()
	}

	return nil
}

type snapshot struct {
	db *KVStore
}

func (f *fsm) Snapshot() (raft.FSMSnapshot, error) {
	return &snapshot{
		db: f.db,
	}, nil
}

func (s *snapshot) Persist(sink raft.SnapshotSink) error {
	defer sink.Close()
	ch := s.db.SnapshotItems()

	for {
		rec := <-ch
		if rec.Key == nil && rec.Value == nil {
			// finished the end
			break
		}

		data, err := proto.Marshal(rec)
		if err != nil {
			return err
		}

		dataSize := uint64(len(data))
		resBuffer := make([]byte, 8)
		binary.LittleEndian.PutUint64(resBuffer, dataSize)
		resBuffer = append(resBuffer, data...)
		if _, err := sink.Write(resBuffer); err != nil {
			return err
		}
	}

	return nil
}

func (s *snapshot) Release() {}

func NewDistDB(dataDir string, config Config) (*DistDB, error) {
	l := &DistDB{
		config: config,
	}
	if err := l.setupDB(dataDir); err != nil {
		return nil, err
	}

	if err := l.setupRaft(dataDir); err != nil {
		return nil, err
	}

	return l, nil
}

func (d *DistDB) setupDB(dataDir string) error {
	dbDir := filepath.Join(dataDir, "db")
	if err := os.MkdirAll(dbDir, 0755); err != nil {
		return err
	}
	var err error
	d.fsm, err = newFsm(dbDir)

	return err
}

func (d *DistDB) Get(k []byte) ([]byte, error) {
	if !d.IsLeader() {
		return nil, ErrNotLeader
	}

	consumeReq := &api.ConsumeRequest{
		Key: k,
	}

	b, err := proto.Marshal(consumeReq)
	if err != nil {
		return nil, err
	}

	action := &api.Action{
		Type:       api.Action_STORE_GET,
		Submessage: b,
	}

	encodedAction, err := proto.Marshal(action)
	if err != nil {
		return nil, err
	}

	future := d.raft.Apply(encodedAction, 10*time.Second).(raft.ApplyFuture)
	if future.Error() != nil {
		if future.Error() == raft.ErrNotLeader {
			return nil, ErrNotLeader
		}
		return nil, future.Error()
	}

	res := future.Response().(*getResponse)
	return res.data, res.err
}

func (d *DistDB) IterateKeysAndPairs() ([]*api.Record, error) {
	return d.fsm.db.IterateKeysAndPairs()
}

func (d *DistDB) ScanWithPrefix(pref []byte) ([]*api.Record, error) {
	return d.fsm.db.ScanWithPrefix(pref)
}

func (d *DistDB) IsLeader() bool {
	return d.raft.State() == raft.Leader
}

func (d *DistDB) Put(rec *api.Record) error {
	if !d.IsLeader() {
		return ErrNotLeader
	}

	data, err := d.apply(api.Action_STORE_POST, &api.ProduceRequest{
		Record: rec,
	})
	if err != nil {
		return err
	}

	putResp := data.(*putResponse)
	return putResp.err
}

func (d *DistDB) apply(ty api.Action_Type, req proto.Message) (interface{}, error) {
	messageBytes, err := proto.Marshal(req)
	if err != nil {
		return nil, err
	}

	action := &api.Action{
		Type:       ty,
		Submessage: messageBytes,
	}

	encodedAction, err := proto.Marshal(action)
	if err != nil {
		return nil, err
	}

	timeout := 10 * time.Second
	future := d.raft.Apply(encodedAction, timeout)
	if future.Error() != nil {
		if future.Error() == raft.ErrNotLeader {
			return nil, ErrNotLeader
		}
		return nil, future.Error()
	}

	res := future.Response()
	if err, ok := res.(error); ok {
		return nil, err
	}

	return res, nil
}

func (d *DistDB) Delete(k []byte) error {
	if d.raft.State() != raft.Leader {
		return raft.ErrNotLeader
	}

	// TODO implement delete
	return nil
}

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, err
	}

	l, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return 0, err
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port, nil
}

func (d *DistDB) setupRaft(dataDir string) error {
	raftPort, err := getFreePort()
	if err != nil {
		return err
	}

	raftAddr := fmt.Sprintf(":%d", raftPort)

	if err := os.Mkdir(filepath.Join(dataDir, "raft"), os.ModePerm); err != nil {
		return err
	}

	stableStore, err := raftboltdb.NewBoltStore(filepath.Join(dataDir, "raft", "raft.db"))
	if err != nil {
		return err
	}

	snapshotStore, err := raft.NewFileSnapshotStore(filepath.Join(
		dataDir, "raft"), 1, os.Stderr)
	if err != nil {
		return err
	}

	maxPool := 5
	timeout := 10 * time.Second

	tcpAddr, err := net.ResolveTCPAddr("tcp", raftAddr)
	if err != nil {
		return err
	}

	transport, err := raft.NewTCPTransport(raftAddr, tcpAddr, maxPool, timeout, os.Stderr)
	if err != nil {
		return err
	}

	config := raft.DefaultConfig()
	config.SnapshotThreshold = 1024
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

	d.raft, err = raft.NewRaft(config, d.fsm, stableStore, stableStore, snapshotStore, transport)
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

func (d *DistDB) Join(id, addr string) error {
	configFuture := d.raft.GetConfiguration()
	if err := configFuture.Error(); err != nil {
		return err
	}

	serverID := raft.ServerID(id)
	serverAddr := raft.ServerAddress(addr)

	for _, srv := range configFuture.Configuration().Servers {
		if srv.ID == serverID || srv.Address == serverAddr {
			if srv.ID == serverID && srv.Address == serverAddr {
				return nil
			}

			removeFuture := d.raft.RemoveServer(serverID, 0, 0)
			if err := removeFuture.Error(); err != nil {
				return err
			}
		}
	}

	addFuture := d.raft.AddVoter(serverID, serverAddr, 0, 0)
	if err := addFuture.Error(); err != nil {
		return err
	}

	return nil
}

func (d *DistDB) Leave(id string) error {
	removeFuture := d.raft.RemoveServer(raft.ServerID(id), 0, 0)
	return removeFuture.Error()
}

func (l *DistDB) WaitForLeader(timeout time.Duration) error {
	timeoutc := time.After(timeout)
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-timeoutc:
			return fmt.Errorf("timed out")
		case <-ticker.C:
			if l := l.raft.Leader(); l != "" {
				return nil
			}
		}
	}
}
