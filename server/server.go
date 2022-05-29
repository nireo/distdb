package server

import (
	"context"
	"strings"
	"time"

	grpc_zap "github.com/grpc-ecosystem/go-grpc-middleware/logging/zap"
	grpc_ctxtags "github.com/grpc-ecosystem/go-grpc-middleware/tags"
	bolt "go.etcd.io/bbolt"
	"go.opencensus.io/plugin/ocgrpc"
	"go.opencensus.io/stats/view"
	"go.opencensus.io/trace"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/auth"
	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

type Authorizer interface {
	Authorize(subject, object, action string) error
}

type Config struct {
	DB         engine.Storage[bolt.DB]
	Authorizer Authorizer
}

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

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
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	if err := s.DB.Put(req.Record.Key, req.Record.Value); err != nil {
		return nil, err
	}

	return &api.ProduceResponse{}, nil
}

func (s *grpcServer) Consume(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponse, error) {

	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	val, err := s.DB.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponse{Value: val}, nil
}

func (s *grpcServer) ConsumeWithKey(ctx context.Context, req *api.ConsumeRequest) (
	*api.ConsumeResponseRecord, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	val, err := s.DB.Get(req.Key)
	if err != nil {
		return nil, err
	}

	return &api.ConsumeResponseRecord{
		Record: &api.Record{
			Key:   req.Key,
			Value: val,
		},
	}, nil
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

func (s *grpcServer) ConsumeStream(req *api.StoreEmptyRequest, stream api.Store_ConsumeStreamServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildcard,
		consumeAction,
	); err != nil {
		return err
	}

	// TODO: This is very bad. Badger provides the badger.Stream class, but
	// that is quite hard to use with this. So do that in the future.
	pairs, err := s.DB.IterateKeysAndPairs()
	if err != nil {
		return err
	}

	idx := 0
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			if idx == len(pairs) {
				// db := s.DB.GetUnderlying()
				// err := db.Subscribe(stream.Context(), func(kv *badger.KVList) error {
				//	for _, kv := range kv.Kv {
				//		if err := stream.Send(&api.ConsumeResponse{
				//			Value: kv.Value,
				//		}); err != nil {
				//			return err
				//		}
				//	}

				//	return nil
				// }, []pb.Match{})

				// if err != nil {
				//	return err
				// }
				return nil
			}
			if err = stream.Send(&api.ConsumeResponse{
				Value: pairs[idx].Value,
			}); err != nil {
				return err
			}
			idx += 1
		}
	}
}

func (s *grpcServer) ConsumeStreamWithKey(req *api.StoreEmptyRequest, stream api.Store_ConsumeStreamWithKeyServer) error {
	if err := s.Authorizer.Authorize(
		subject(stream.Context()),
		objectWildcard,
		consumeAction,
	); err != nil {
		return err
	}

	// TODO: This is very bad. Badger provides the badger.Stream class, but
	// that is quite hard to use with this. So do that in the future.
	pairs, err := s.DB.IterateKeysAndPairs()
	if err != nil {
		return err
	}

	idx := 0
	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
			if idx == len(pairs) {
				// db := s.DB.GetUnderlying()
				// err := db.Subscribe(stream.Context(), func(kv *badger.KVList) error {
				//	for _, kv := range kv.Kv {
				//		if err := stream.Send(&api.ConsumeResponseRecord{
				//			Record: &api.Record{
				//				Key:   kv.Key,
				//				Value: kv.Value,
				//			},
				//		}); err != nil {
				//			return err
				//		}
				//	}

				//	return nil
				// }, []pb.Match{})

				if err != nil {
					return err
				}
				return nil
			}
			if err = stream.Send(&api.ConsumeResponseRecord{
				Record: &api.Record{
					Key:   pairs[idx].Key,
					Value: pairs[idx].Value,
				},
			}); err != nil {
				return err
			}
			idx += 1
		}
	}
}

func (s *grpcServer) AllKeysAndValues(ctx context.Context, req *api.StoreEmptyRequest) (
	*api.MultipleConsume, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	records, err := s.DB.IterateKeysAndPairs()
	if err != nil {
		return nil, err
	}

	return &api.MultipleConsume{Pairs: records}, nil
}

func (s *grpcServer) PrefixConsume(ctx context.Context, req *api.Prefix) (
	*api.MultipleConsume, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	records, err := s.DB.ScanWithPrefix(req.Prefix)
	if err != nil {
		return nil, err
	}

	return &api.MultipleConsume{Pairs: records}, nil
}

func NewGRPCServer(config *Config, grpcOpts ...grpc.ServerOption) (*grpc.Server, error) {
	logger := zap.L().Named("server")
	zapOpts := []grpc_zap.Option{
		grpc_zap.WithDurationField(
			func(duration time.Duration) zapcore.Field {
				return zap.Int64("grpc.time_ns", duration.Microseconds())
			},
		),
	}

	trace.ApplyConfig(trace.Config{DefaultSampler: trace.AlwaysSample()})
	err := view.Register(ocgrpc.DefaultServerViews...)
	if err != nil {
		return nil, err
	}

	halfSampler := trace.ProbabilitySampler(0.5)
	trace.ApplyConfig(trace.Config{
		DefaultSampler: func(p trace.SamplingParameters) trace.SamplingDecision {
			if strings.Contains(p.Name, "Produce") {
				return trace.SamplingDecision{Sample: true}
			}
			return halfSampler(p)
		},
	})

	grpcOpts = append(grpcOpts,
		grpc.StreamInterceptor(
			grpc_middleware.ChainStreamServer(
				grpc_ctxtags.StreamServerInterceptor(),
				grpc_zap.StreamServerInterceptor(logger, zapOpts...),
				grpc_auth.StreamServerInterceptor(authenticate),
			)), grpc.UnaryInterceptor(grpc_middleware.ChainUnaryServer(
			grpc_ctxtags.UnaryServerInterceptor(),
			grpc_zap.UnaryServerInterceptor(logger, zapOpts...),
			grpc_auth.UnaryServerInterceptor(authenticate),
		)),
		grpc.StatsHandler(&ocgrpc.ServerHandler{}),
	)

	gsrv := grpc.NewServer(grpcOpts...)
	srv, err := newgrpcServer(config)
	if err != nil {
		return nil, err
	}
	api.RegisterStoreServer(gsrv, srv)
	return gsrv, nil
}

func authenticate(ctx context.Context) (context.Context, error) {
	peer, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "couldn't find peer info").Err()
	}

	if peer.AuthInfo == nil {
		return context.WithValue(ctx, subjectContextKey{}, ""), nil
	}

	tlsInfo := peer.AuthInfo.(credentials.TLSInfo)
	subject := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectContextKey{}, subject)

	return ctx, nil
}

func subject(ctx context.Context) string {
	return ctx.Value(subjectContextKey{}).(string)
}

type subjectContextKey struct{}
