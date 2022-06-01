package engine_test

import (
	"bytes"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/engine"
	"github.com/stretchr/testify/require"
)

func getFreePort() (int, error) {
	addr, err := net.ResolveTCPAddr("tcp", "localhost:0")
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

func TestMultipleNodes(t *testing.T) {
	var dbs []*engine.DistDB
	var err error
	nodeCount := 3

	ports := make([]int, nodeCount)
	for i := 0; i < nodeCount; i++ {
		ports[i], err = getFreePort()
		require.NoError(t, err)
	}

	for i := 0; i < nodeCount; i++ {
		dataDir, err := ioutil.TempDir("", "distributed-log-test")
		require.NoError(t, err)

		defer func(dir string) {
			_ = os.RemoveAll(dir)
		}(dataDir)

		ln, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", ports[i]))
		require.NoError(t, err)

		config := engine.Config{}
		config.Raft.StreamLayer = engine.NewStreamLayer(ln, nil, nil)
		config.Raft.LocalID = raft.ServerID(fmt.Sprintf("%d", i))
		config.Raft.HeartbeatTimeout = 50 * time.Millisecond
		config.Raft.ElectionTimeout = 50 * time.Millisecond
		config.Raft.LeaderLeaseTimeout = 50 * time.Millisecond
		config.Raft.CommitTimeout = 5 * time.Millisecond

		if i == 0 {
			config.Raft.Bootstrap = true
		}

		l, err := engine.NewDistDB(dataDir, config)
		require.NoError(t, err)

		if i != 0 {
			err = dbs[0].Join(fmt.Sprintf("%d", i), ln.Addr().String())
			require.NoError(t, err)
		} else {
			err = l.WaitForLeader(3 * time.Second)
			require.NoError(t, err)
		}

		dbs = append(dbs, l)
	}

	records := []*api.Record{
		{
			Key:   []byte("hello"),
			Value: []byte("world"),
		},
		{
			Key:   []byte("world"),
			Value: []byte("hello"),
		},
	}

	for _, rec := range records {
		err := dbs[0].Put(rec)
		require.NoError(t, err)

		require.Eventually(t, func() bool {
			for j := 0; j < nodeCount; j++ {
				val, err := dbs[j].Get(rec.Key)
				if err != nil {
					return false
				}

				if !bytes.Equal(val, rec.Value) {
					return false
				}
			}
			return true
		}, 500*time.Millisecond, 50*time.Millisecond)

	}

	err = dbs[0].Leave("1")
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	err = dbs[0].Put(&api.Record{
		Key:   []byte("hellohello"),
		Value: []byte("worldworld"),
	})
	require.NoError(t, err)

	time.Sleep(50 * time.Millisecond)

	val, err := dbs[1].Get([]byte("hellohello"))
	require.IsType(t, api.ErrKeyNotFound{}, err)
	require.Nil(t, val)

	val, err = dbs[2].Get([]byte("hellohello"))
	require.NoError(t, err)
	require.Equal(t, []byte("worldworld"), val)
}
