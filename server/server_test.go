package server

import (
	"bytes"
	"context"
	"flag"
	"io/ioutil"
	"net"
	"os"
	"testing"
	"time"

	api "github.com/nireo/distdb/api/v1"
	"github.com/nireo/distdb/auth"
	"github.com/nireo/distdb/config"
	"github.com/nireo/distdb/engine"
	"go.opencensus.io/examples/exporter"
	"go.uber.org/zap"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
)

var debug = flag.Bool("debug", false, "Enable observability for debugging.")

func TestMain(m *testing.M) {
	flag.Parse()

	if *debug {
		logger, err := zap.NewDevelopment()
		if err != nil {
			panic(err)
		}
		zap.ReplaceGlobals(logger)
	}

	os.Exit(m.Run())
}

func handleErr(t *testing.T, err error) {
	if err != nil {
		t.Fatal(err)
	}
}

func TestServer(t *testing.T) {
	for scenario, fn := range map[string]func(
		t *testing.T,
		rootClient api.StoreClient,
		nobodyClient api.StoreClient,
		config *Config,
	){
		"produce/consume a record into the store":     testProduceConsume,
		"consume fails when requesting not found key": testConsumeNonExistant,
		"product":            testProduceConsumeStream,
		"unauthorized fails": testUnauthorized,
	} {
		t.Run(scenario, func(t *testing.T) {
			rootClient, nobodyClient, config, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, rootClient, nobodyClient, config)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	rootClient api.StoreClient,
	nobodyClient api.StoreClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()

	l, err := net.Listen("tcp", "127.0.0.1:0")
	handleErr(t, err)

	newClient := func(crtPath, keyPath string) (
		*grpc.ClientConn,
		api.StoreClient,
		[]grpc.DialOption,
	) {
		tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
			CertFile: crtPath,
			KeyFile:  keyPath,
			CAFile:   config.CAFile,
			Server:   false,
		})
		handleErr(t, err)

		tlsCreds := credentials.NewTLS(tlsConfig)
		opts := []grpc.DialOption{grpc.WithTransportCredentials(tlsCreds)}
		conn, err := grpc.Dial(l.Addr().String(), opts...)
		handleErr(t, err)
		client := api.NewStoreClient(conn)

		return conn, client, opts
	}

	var rootConn *grpc.ClientConn
	rootConn, rootClient, _ = newClient(
		config.RootClientCertFile,
		config.RootClientKeyFile,
	)
	var nobodyConn *grpc.ClientConn
	nobodyConn, nobodyClient, _ = newClient(
		config.NobodyClientCertFile,
		config.NobodyClientKeyFile,
	)

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

	authorizer := auth.New(config.ACLModelFile, config.ACLPolicyFile)

	var telemetryExporter *exporter.LogExporter
	if *debug {
		metricsLogFile, err := ioutil.TempFile("", "metrics-*.log")
		handleErr(t, err)
		t.Logf("metrics log file: %s", metricsLogFile.Name())

		tracesLogFile, err := ioutil.TempFile("", "traces-*.log")
		handleErr(t, err)
		t.Logf("traces log file: %s", tracesLogFile.Name())

		telemetryExporter, err = exporter.NewLogExporter(exporter.Options{
			MetricsLogFile:    metricsLogFile.Name(),
			TracesLogFile:     tracesLogFile.Name(),
			ReportingInterval: time.Second,
		})
		handleErr(t, err)
		err = telemetryExporter.Start()
		handleErr(t, err)
	}

	cfg = &Config{
		DB:         cdb,
		Authorizer: authorizer,
	}

	if fn != nil {
		fn(cfg)
	}
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
	handleErr(t, err)

	go func() {
		server.Serve(l)
	}()

	return rootClient, nobodyClient, cfg, func() {
		server.Stop()
		rootConn.Close()
		nobodyConn.Close()
		l.Close()
		cdb.Close()

		if telemetryExporter != nil {
			time.Sleep(1500 * time.Millisecond)
			telemetryExporter.Close()
			telemetryExporter.Stop()
		}
	}
}

func testProduceConsume(t *testing.T, client, _ api.StoreClient, config *Config) {
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

func testConsumeNonExistant(t *testing.T, client, _ api.StoreClient, config *Config) {
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

func testProduceConsumeStream(t *testing.T, client, _ api.StoreClient, config *Config) {
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

func testUnauthorized(t *testing.T, _, client api.StoreClient, config *Config) {
	ctx := context.Background()
	produce, err := client.Produce(ctx, &api.ProduceRequest{
		Record: &api.Record{
			Key:   []byte("hello"),
			Value: []byte("value"),
		},
	})
	if produce != nil {
		t.Fatalf("produce response should be nil")
	}

	gotCode, wantCode := status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}

	consume, err := client.Consume(ctx, &api.ConsumeRequest{
		Key: []byte("hello"),
	})

	if consume != nil {
		t.Fatalf("consume response should be nil")
	}

	gotCode, wantCode = status.Code(err), codes.PermissionDenied
	if gotCode != wantCode {
		t.Fatalf("got code: %d, want: %d", gotCode, wantCode)
	}
}
