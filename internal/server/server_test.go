package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/Devin-Yeung/proglog/internal/config"
	"github.com/Devin-Yeung/proglog/internal/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func TestLog(t *testing.T) {
	type testCase struct {
		name string
		fn   func(t *testing.T, client api.LogClient)
	}

	for _, tc := range []testCase{
		{name: "produce/consume", fn: testProduceConsume},
		{name: "consume stream", fn: testConsumeStream},
		{name: "produce stream", fn: testProduceStream},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client, tearDown := setupTestServer(t)
			defer tearDown()
			tc.fn(t, client)
		})
	}
}

func setupTestServer(t *testing.T) (client api.LogClient, tearDown func()) {
	t.Helper()

	// setup a tcp listener on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// setup TLS credentials for the client
	clientTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ClientCertFile,
		KeyFile:  config.ClientKeyFile,
		CAFile:   config.CAFile,
	})
	require.NoError(t, err)

	clientCreds := credentials.NewTLS(clientTLSConfig)

	// setup gRPC client connection
	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(clientCreds),
	}

	conn, err := grpc.NewClient(
		listener.Addr().String(),
		clientOptions...,
	)
	require.NoError(t, err)

	// create an underlying log
	cfg := log.NewConfig()
	log, err := log.NewLog(
		t.TempDir(),
		*cfg,
	)
	require.NoError(t, err)

	// setup TLS credentials for the server
	serverTLSConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: config.ServerCertFile,
		KeyFile:  config.ServerKeyFile,
		CAFile:   config.CAFile,
		Server:   true,
	})
	require.NoError(t, err)

	serverCreds := credentials.NewTLS(serverTLSConfig)
	// create a new gRPC server and spin it up
	server, err := NewGRPCServer(&Config{CommitLog: log}, grpc.Creds(serverCreds))
	require.NoError(t, err)

	client = api.NewLogClient(conn)

	go func() {
		server.Serve(listener)
	}()

	tearDown = func() {
		// close the client connection
		conn.Close()
		// close the server
		server.Stop()
		// close the listener
		listener.Close()
		// close the log
		log.Close()
	}

	return
}

func testProduceConsume(t *testing.T, client api.LogClient) {
	ctx := context.Background()

	want := &api.Record{
		Value: []byte("hello world"),
	}

	produceResp, err := client.Produce(
		ctx,
		&api.ProduceRequest{Record: want},
	)
	require.NoError(t, err)
	assert.Equal(t, produceResp.Offset, uint64(0))

	consumeResp, err := client.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produceResp.Offset},
	)
	require.NoError(t, err)
	assert.Equal(t, want.Value, consumeResp.Record.Value)
	assert.Equal(t, produceResp.Offset, consumeResp.Record.Offset)
}

func testConsumeStream(t *testing.T, client api.LogClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	// spin a new go routine to produce records
	go func() {
		for i := 0; i < 10000; i++ {
			record := &api.Record{
				Value:  []byte(fmt.Sprintf("offset %d", i)),
				Offset: uint64(i),
			}

			_, err := client.Produce(
				ctx,
				&api.ProduceRequest{Record: record},
			)
			require.NoError(t, err)
		}
	}()

	consumeStream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	require.NoError(t, err)

	// should always receive all records
	for i := 0; i < 10000; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("test timed out")
		default:
			_, err := consumeStream.Recv()
			assert.NoError(t, err)
		}
	}
}

func testProduceStream(t *testing.T, client api.LogClient) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	stream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	const recordCount = 10000

	var g errgroup.Group

	// spin a go routine to produce records
	g.Go(func() error {
		for i := 0; i < recordCount; i++ {
			want := &api.Record{
				Value: []byte(fmt.Sprintf("record %d", i)),
			}

			if err := stream.Send(&api.ProduceRequest{Record: want}); err != nil {
				return err
			}
		}
		return stream.CloseSend()
	})

	// spin a go routine to receive produced record offsets
	g.Go(func() error {
		for i := 0; i < recordCount; i++ {
			resp, err := stream.Recv()
			if err != nil {
				return err
			}
			assert.Equal(t, uint64(i), resp.Offset)
		}

		_, err := stream.Recv()
		if !assert.ErrorIs(t, err, io.EOF) {
			return err
		}

		return nil
	})

	require.NoError(t, g.Wait())
}
