package engine

import (
	"context"
	"sync"

	api "github.com/nireo/distdb/api/v1"
	"go.uber.org/zap"
	"google.golang.org/grpc"
)

type Replicator struct {
	DialOptions []grpc.DialOption
	LocalServer api.StoreServer
	logger      *zap.Logger
	mu          sync.Mutex
	servers     map[string]chan struct{}
	closed      bool
	close       chan struct{}
}

func (r *Replicator) Join(name, addr string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if r.closed {
		return nil
	}

	if _, ok := r.servers[name]; ok {
		return nil
	}
	r.servers[name] = make(chan struct{})
	return nil
}

func (r *Replicator) replicate(addr string, leave chan struct{}) {
	cc, err := grpc.Dial(addr, r.DialOptions...)
	if err != nil {
		r.logError(err, "failed to dial", addr)
		return
	}
	defer cc.Close()

	client := api.NewStoreClient(cc)
	ctx := context.Background()

}
