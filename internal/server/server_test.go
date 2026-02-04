package server

import (
	"context"
	"fmt"
	"net"
	"testing"
	"time"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/Devin-Yeung/proglog/internal/log"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func TestLog(t *testing.T) {
	type testCase struct {
		name string
		fn   func(t *testing.T, client api.LogClient)
	}

	for _, tc := range []testCase{
		{name: "produce/consume", fn: testProduceConsume},
		{name: "consume stream", fn: testConsumeStream},
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
	listener, err := net.Listen("tcp", ":0")
	require.NoError(t, err)

	// setup gRPC client connection
	clientOptions := []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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

	// create a new gRPC server and spin it up
	server, err := NewGRPCServer(&Config{CommitLog: log})
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
