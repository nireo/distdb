package server

import (
	"context"

	"github.com/dgraph-io/badger/v3"
	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/engine"
	"google.golang.org/grpc"
)

type Config struct {
	DB engine.Storage[badger.DB]
}

var _ api.StoreServer = (*grpcServer)(nil)

type grpcServer struct {
	api.UnimplementedStoreServer
	*Config
}

func newgrpcServer(config *Config) (srv *grpcServer, err error) {
	srv = &grpcServer{
		Config: config,
	}
	err = nil
	return
}

func (s *grpcServer) Produce(ctx context.Context, req *api.ProduceRequest) (
	*api.ProduceResponse, error) {
	if err := s.DB.Put(req.Record.Key, req.Record.Value); err != nil {
		return nil, err
	}

	return &api.ProduceResponse{}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {
	val, err := s.DB.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Value: val}, nil
}

func (s *grpcServer) ProduceStream(stream api.Store_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			return err
		}

		res, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err = stream.Send(res); err != nil {
			return err
		}
	}
}

func (s *grpcServer) ConsumeStream(req *api.ConsumeRequest, stream api.Store_ConsumeStreamServer) error {
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			res, err := s.Consume(stream.Context(), req)
			switch err.(type) {
			case nil:
			default:
				return err
			}

			if err = stream.Send(res); err != nil {
				return err
			}
		}
	}
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterStoreServer(gsrv, srv)
	return gsrv, nil
}
