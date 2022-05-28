package agent_test

import (
	"context"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	"github.com/nireo/distdb/agent"
	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/config"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func client(t *testing.T, agent *agent.Agent, tlsConfig *tls.Config) api.StoreClient {
	tlsCreds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
	rpcAddr, err := agent.Config.RPCAddr()
	require.NoError(t, err)
	conn, err := grpc.Dial(fmt.Sprintf(
		"%s",
		rpcAddr,
	), opts...)
	require.NoError(t, err)
	client := api.NewStoreClient(conn)
	return client
}

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

func TestAgent(t *testing.T) {
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		Server:        true,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	peerTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.RootClientCertFile,
		KeyFile:       config.RootClientKeyFile,
		CAFile:        config.CAFile,
		Server:        false,
		ServerAddress: "127.0.0.1",
	})
	require.NoError(t, err)

	var agents []*agent.Agent
	for i := 0; i < 3; i++ {
		port1, err := getFreePort()
		require.NoError(t, err)

		port2, err := getFreePort()
		require.NoError(t, err)

		bindAddr := fmt.Sprintf("%s:%d", "127.0.0.1", port1)

		dataDir, err := ioutil.TempDir("", "agent-test-store")
		require.NoError(t, err)

		var startJoinAddrs []string
		if i != 0 {
			startJoinAddrs = append(startJoinAddrs, agents[0].Config.BindAddr)
		}

		agent, err := agent.New(agent.Config{
			NodeName:        fmt.Sprintf("%d", i),
			StartJoinAddrs:  startJoinAddrs,
			BindAddr:        bindAddr,
			RPCPort:         port2,
			DataDir:         dataDir,
			ACLModelFile:    config.ACLModelFile,
			ACLPolicyFile:   config.ACLPolicyFile,
			ServerTLSConfig: serverTLSConfig,
			PeerTLSConfig:   peerTLSConfig,
		})
		require.NoError(t, err)
		agents = append(agents, agent)
	}

	defer func() {
		for _, agent := range agents {
			err := agent.Shutdown()
			require.NoError(t, err)

			require.NoError(t, os.RemoveAll(agent.Config.DataDir))
		}
	}()

	time.Sleep(3 * time.Second)

	leaderClient := client(t, agents[0], peerTLSConfig)
	_, err = leaderClient.Produce(
		context.Background(),
		&api.ProduceRequest{
			Record: &api.Record{
				Key:   []byte("hello"),
				Value: []byte("world"),
			},
		},
	)
	require.NoError(t, err)

	consumeResponse, err := leaderClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Key: []byte("hello"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Value, []byte("world"))

	time.Sleep(3 * time.Second)

	followerClient := client(t, agents[1], peerTLSConfig)
	consumeResponse, err = followerClient.Consume(
		context.Background(),
		&api.ConsumeRequest{
			Key: []byte("hello"),
		},
	)
	require.NoError(t, err)
	require.Equal(t, consumeResponse.Value, []byte("world"))
}
