package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/config"
	"github.com/nireo/distdb/engine"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func handleErr(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		client api.StoreClient,
		config *Config,
	){
		"produce/consume a record into the store":     testProduceConsume,
		"consume fails when requesting not found key": testConsumeNonExistant,
		"product": testProduceConsumeStream,
	} {
		t.Run(scenario, func(t *testing.T) {
			client, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.StoreClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	handleErr(t, err)

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
	})
	handleErr(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(l.Addr().String(), grpc.WithTransportCredentials(clientCreds))
	if err != nil {
		t.Fatal(err)
	}
	client = api.NewStoreClient(cc)

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		CAFile:        config.CAFile,
		ServerAddress: l.Addr().String(),
		Server:        true,
	})
	handleErr(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)

	dir, err := ioutil.TempDir("", "server-test")
	handleErr(t, err)

	cdb, err := engine.NewKVStoreWithPath(dir)
	handleErr(t, err)

	cfg = &Config{
		DB: cdb,
	}

	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	handleErr(t, err)

	go func() {
		server.Serve(l)
	}()

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		l.Close()
		cdb.Close()
	}
}

func testProduceConsume(t *testing.T, client api.StoreClient, config *Config) {
	ctx := context.Background()

	want := &api.Record{
		Key:   []byte("hello"),
		Value: []byte("world"),
	}

	_, err := client.Produce(ctx, &api.ProduceRequest{Record: want})
	handleErr(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Key: []byte("hello"),
	})
	handleErr(t, err)

	if !bytes.Equal(want.Value, consume.Value) {
		t.Fatalf("want value and consume value are not equal. got=%s want=%s",
			string(consume.Value), string(consume.Value))
	}
}

func testConsumeNonExistant(t *testing.T, client api.StoreClient, config *Config) {
	ctx := context.Background()
	_, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Key:   []byte("hello"),
			Value: []byte("world"),
		},
	})
	handleErr(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Key: []byte("nonexistant"),
	})
	if consume != nil {
		t.Fatal("consume not nil")
	}

	got := grpc.Code(err)
	want := grpc.Code(api.ErrKeyNotFound{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err: %v, want %v", got, want)
	}
}

func testProduceConsumeStream(t *testing.T, client api.StoreClient, config *Config) {
	ctx := context.Background()

	records := []*api.Record{{
		Key:   []byte("hello"),
		Value: []byte("world"),
	}, {
		Key:   []byte("world"),
		Value: []byte("hello"),
	}}

	{
		stream, err := client.ProduceStream(ctx)
		handleErr(t, err)

		for _, record := range records {
			err = stream.Send(&api.ProduceRequest{
				Record: record,
			})
			handleErr(t, err)

			_, err := stream.Recv()
			handleErr(t, err)
		}
	}
	// {
	//	stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Key: []byte("hello")})
	//	handleErr(t, err)

	//	for _, record := range records {
	//		res, err := stream.Recv()
	//		handleErr(t, err)

	//		if !bytes.Equal(res.Value, record.Value) {
	//			t.Fatalf("not equal values. got=%s want=%s", string(record.Value), string(res.Value))
	//		}
	//	}
	// }
}
