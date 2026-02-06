package server

import (
	"context"
	"fmt"
	"io"
	"net"
	"testing"
	"time"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/Devin-Yeung/proglog/internal/auth"
	"github.com/Devin-Yeung/proglog/internal/config"
	"github.com/Devin-Yeung/proglog/internal/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/status"
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
			rootClient, _, tearDown := setupTestClients(t)
			defer tearDown()
			tc.fn(t, rootClient)
		})
	}
}

func TestLogAuth(t *testing.T) {
	type testCase struct {
		name string
		fn   func(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient)
	}
	for _, tc := range []testCase{
		{name: "unauthorized client can't consume/produce", fn: testUnauthorized},
	} {
		t.Run(tc.name, func(t *testing.T) {
			rootClient, nobodyClient, tearDown := setupTestClients(t)
			defer tearDown()
			tc.fn(t, rootClient, nobodyClient)
		})
	}
}

// setupClient is a helper function to create a gRPC client connection with specified TLS credentials.
// It returns a gRPC client connection and a LogClient for making RPC calls to the server.
func setupClient(
	t *testing.T,
	certFile string,
	keyFile string,
	addr string,
) (*grpc.ClientConn, api.LogClient) {
	t.Helper()

	tlsConfig, err := config.SetupTLSConfig(config.TLSConfig{
		CertFile: certFile,
		KeyFile:  keyFile,
		CAFile:   config.CAFile,
		Server:   false,
	})
	require.NoError(t, err)

	// create TLS credentials for the client
	creds := credentials.NewTLS(tlsConfig)
	opts := []grpc.DialOption{
		grpc.WithTransportCredentials(creds),
	}
	// dial the server with the TLS credentials
	conn, err := grpc.NewClient(addr, opts...)
	require.NoError(t, err)

	client := api.NewLogClient(conn)
	return conn, client
}

// setupServer is a helper function to create and start a gRPC server with TLS credentials.
// It returns a net.Listener for the server, the gRPC server instance, and a tearDown function to clean up resources after the test.
func setupServer(t *testing.T) (listener net.Listener, server *grpc.Server, tearDown func()) {
	t.Helper()

	// setup a tcp listener on a random port
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	// create an underlying log
	cfg := log.NewConfig()
	commitLog, err := log.NewLog(
		t.TempDir(),
		*cfg,
	)
	require.NoError(t, err)

	// create an authorizer
	authorizer, err := auth.NewAuthorizer(
		config.ACLModelFile,
		config.ACLPolicyFile,
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
	server, err = NewGRPCServer(
		&Config{
			CommitLog:  commitLog,
			Authorizer: authorizer,
		},
		grpc.Creds(serverCreds),
	)
	require.NoError(t, err)

	tearDown = func() {
		// close the server
		server.Stop()
		// close the listener
		listener.Close()
		// close the log
		commitLog.Close()
	}

	return listener, server, tearDown
}

func setupTestClients(t *testing.T) (rootClient api.LogClient, nobodyClient api.LogClient, tearDown func()) {
	t.Helper()

	listener, server, serverTearDown := setupServer(t)

	go func() {
		server.Serve(listener)
	}()

	rootClientConn, rootClient := setupClient(
		t,
		config.RootCertFile,
		config.RootKeyFile,
		listener.Addr().String(),
	)

	nobodyClientConn, nobodyClient := setupClient(
		t,
		config.NobodyCertFile,
		config.NobodyKeyFile,
		listener.Addr().String(),
	)

	tearDown = func() {
		// close the client connection
		rootClientConn.Close()
		nobodyClientConn.Close()
		serverTearDown()
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

	const recordCount = 10000

	produceStream, err := client.ProduceStream(ctx)
	require.NoError(t, err)

	// spin a new go routine to produce records
	var g errgroup.Group
	g.Go(func() error {
		for i := 0; i < recordCount; i++ {
			record := &api.Record{
				Value:  []byte(fmt.Sprintf("offset %d", i)),
				Offset: uint64(i),
			}

			if err := produceStream.Send(&api.ProduceRequest{Record: record}); err != nil {
				return err
			}
		}

		return produceStream.CloseSend()
	})

	consumeStream, err := client.ConsumeStream(
		ctx,
		&api.ConsumeRequest{Offset: 0},
	)
	require.NoError(t, err)

	// should always receive all records
	for i := 0; i < recordCount; i++ {
		select {
		case <-ctx.Done():
			t.Fatal("test timed out")
		default:
			_, err := consumeStream.Recv()
			assert.NoError(t, err)
		}
	}

	require.NoError(t, g.Wait())
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

func testUnauthorized(t *testing.T, rootClient api.LogClient, nobodyClient api.LogClient) {
	ctx := context.Background()

	produceResp, err := rootClient.Produce(
		ctx,
		&api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}},
	)
	require.NoError(t, err)

	_, err = nobodyClient.Produce(
		ctx,
		&api.ProduceRequest{Record: &api.Record{Value: []byte("hello world")}},
	)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))

	_, err = nobodyClient.Consume(
		ctx,
		&api.ConsumeRequest{Offset: produceResp.Offset},
	)
	require.Error(t, err)
	assert.Equal(t, codes.PermissionDenied, status.Code(err))
}
