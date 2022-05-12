package server

import (
	"context"
	"github.com/jxofficial/log/internal/log"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/jxofficial/log/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestServer(t *testing.T) {
	tests := map[string]func(
		t *testing.T,
		client api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"consume past boundary fails":                        testConsumePastBoundary,
	}

	for scenario, fn := range tests {
		t.Run(scenario, func(t *testing.T) {
			client, cfg, teardown := setupTest(t, nil)
			defer teardown()
			fn(t, client, cfg)
		})
	}
}

func setupTest(t *testing.T, fn func(*Config)) (
	client api.LogClient,
	cfg *Config,
	teardown func(),
) {
	t.Helper()
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	clientOptions := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	cc, err := grpc.Dial(listener.Addr().String(), clientOptions...)
	require.NoError(t, err)

	dir, err := ioutil.TempDir("", "server_test")
	require.NoError(t, err)

	clog, err := log.NewLog(dir, log.Config{})
	require.NoError(t, err)

	cfg = &Config{
		CommitLog: clog,
	}
	// Manipulate the server config.
	if fn != nil {
		fn(cfg)
	}

	server, err := NewGRPCServer(cfg)
	require.NoError(t, err)

	go func() {
		server.Serve(listener)
	}()

	client = api.NewLogClient(cc)

	return client, cfg, func() {
		server.Stop()
		cc.Close()
		listener.Close()
		clog.Remove()
	}
}

func testProduceConsume(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	r := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: r})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset})
	require.NoError(t, err)
	require.Equal(t, r.Value, consume.Record.Value)
	require.Equal(t, r.Offset, consume.Record.Offset)
}

func testConsumePastBoundary(t *testing.T, client api.LogClient, cfg *Config) {
	ctx := context.Background()
	r := &api.Record{Value: []byte("hello world")}

	produce, err := client.Produce(ctx, &api.ProduceRequest{Record: r})
	require.NoError(t, err)

	consume, err := client.Consume(ctx, &api.ConsumeRequest{Offset: produce.Offset + 1})
	if consume != nil {
		t.Fatal("consume out of bounds not nil")
	}
	got := status.Code(err)
	want := status.Code(api.ErrOffsetOutOfRange{}.GRPCStatus().Err())
	if got != want {
		t.Fatalf("got err status code: %v, want %v", got, want)
	}
}