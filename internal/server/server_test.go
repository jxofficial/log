package server

import (
	"context"
	"github.com/jxofficial/log/internal/config"
	"github.com/jxofficial/log/internal/log"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
	"io/ioutil"
	"net"
	"testing"

	api "github.com/jxofficial/log/api/v1"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestServer(t *testing.T) {
	tests := map[string]func(
		t *testing.T,
		client api.LogClient,
		cfg *Config,
	){
		"produce/consume a message to/from the log succeeds": testProduceConsume,
		"consume past boundary fails":                        testConsumePastBoundary,
		"produce/consume stream works":                       testProduceConsumeStream,
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

	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CAFile:   config.CAFile,
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
	})
	require.NoError(t, err)
	clientCreds := credentials.NewTLS(clientTLSConfig)
	cc, err := grpc.Dial(listener.Addr().String(), grpc.WithTransportCredentials(clientCreds))
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

	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile:      config.ServerCertFile,
		KeyFile:       config.ServerKeyFile,
		ServerAddress: listener.Addr().String(),
		IsServer:      true,
		// Both server and client use the same CA which contains both client and server certs.
		// But for the server's *tls.Config, the CA is not assigned to rootCA but clientCA.
		CAFile: config.CAFile,
	})
	require.NoError(t, err)
	serverCreds := credentials.NewTLS(serverTLSConfig)
	server, err := NewGRPCServer(cfg, grpc.Creds(serverCreds))
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

func testProduceConsumeStream(
	t *testing.T,
	client api.LogClient,
	cfg *Config,
) {
	ctx := context.Background()
	rr := []*api.Record{
		{
			Value:  []byte("first message"),
			Offset: 0,
		},
		{
			Value:  []byte("second message"),
			Offset: 1,
		},
	}

	{
		stream, err := client.ProduceStream(ctx)
		require.NoError(t, err)

		for offset, r := range rr {
			err = stream.Send(&api.ProduceRequest{Record: r})
			require.NoError(t, err)
			resp, err := stream.Recv()
			require.NoError(t, err)
			if resp.Offset != uint64(offset) {
				t.Fatalf("got offset: %d, want: %d", resp.Offset, offset)
			}
		}
	}

	{
		// Start streaming from offset 0.
		stream, err := client.ConsumeStream(ctx, &api.ConsumeRequest{Offset: 0})
		require.NoError(t, err)

		for i, r := range rr {
			resp, err := stream.Recv()
			require.NoError(t, err)
			require.Equal(t, resp.Record, &api.Record{
				Value:  r.Value,
				Offset: uint64(i),
			})
		}
	}
}
