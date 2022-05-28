package agent

import (
	"crypto/tls"
	"fmt"
	"net"
	"sync"

	"github.com/nireo/distdb/auth"
	"github.com/nireo/distdb/discovery"
	"github.com/nireo/distdb/engine"
	"github.com/nireo/distdb/server"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

type Config struct {
	ServerTLSConfig *tls.Config
	PeerTLSConfig   *tls.Config
	DataDir         string
	BindAddr        string
	RPCPort         int
	NodeName        string
	StartJoinAddrs  []string
	ACLModelFile    string
	ACLPolicyFile   string
}

func (c Config) RPCAddr() (string, error) {
	host, _, err := net.SplitHostPort(c.BindAddr)
	if err != nil {
		return "", err
	}

	return fmt.Sprintf("%s:%d", host, c.RPCPort), nil
}

// The agent takes care of managing multiple processes and putting
// different components together.
type Agent struct {
	Config

	store      *engine.KVStore
	replicator *engine.Replicator

	server     *grpc.Server
	membership *discovery.Membership

	shutdown     bool
	shutdowns    chan struct{}
	shutdownLock sync.Mutex
}

func (a *Agent) setupLogger() error {
	logger, err := zap.NewDevelopment()
	if err != nil {
		return err
	}

	zap.ReplaceGlobals(logger)
	return nil
}

func (a *Agent) setupStore() error {
	var err error
	a.store, err = engine.NewKVStoreWithPath(a.Config.DataDir)
	return err
}

func (a *Agent) setupServer() error {
	authorizer := auth.New(a.Config.ACLModelFile, a.Config.ACLPolicyFile)
	serverConfig := &server.Config{
		DB:         a.store,
		Authorizer: authorizer,
	}

	var opts []grpc.ServerOption
	if a.Config.ServerTLSConfig != nil {
		creds := credentials.NewTLS(a.Config.ServerTLSConfig)
		opts = append(opts, grpc.Creds(creds))
	}

	var err error
	a.server, err = server.NewGRPCServer(serverConfig, opts...)
	if err != nil {
		return err
	}

	// rpcAddr, err := a.RPCAddr()
	// if err != nil {
	//	return err
	// }

	// list, err := net.Listen("tcp", rpcAddr)
	// if err != nil {
	//	return err
	// }
	return err
}
