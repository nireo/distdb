package engine_test

import (
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
}
