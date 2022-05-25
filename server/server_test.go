package server

import (
	"bytes"
	"context"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/engine"
	"google.golang.org/grpc"
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

	l, err := net.Listen("tcp", ":0")
	if err != nil {
		t.Fatal(err)
	}

	clientOpts := []grpc.DialOption{grpc.WithInsecure()}
	cc, err := grpc.Dial(l.Addr().String(), clientOpts...)
	if err != nil {
		t.Fatal(err)
	}

	dir, err := ioutil.TempDir("", "server-test")
	if err != nil {
		t.Fatal(err)
	}

	cdb, err := engine.NewKVStoreWithPath(dir)
	if err != nil {
		t.Fatal(err)
	}

	cfg = &Config{
		DB: cdb,
	}

	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg)
	if err != nil {
		t.Fatal(err)
	}

	go func() {
		server.Serve(l)
	}()

	client = api.NewStoreClient(cc)
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
