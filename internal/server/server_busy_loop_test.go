package server

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	api "github.com/Devin-Yeung/proglog/api/v1"
	commitlog "github.com/Devin-Yeung/proglog/internal/log"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
)

func TestConsumeStreamDoesNotSpinOnOffsetOutOfRange(t *testing.T) {
	const (
		observeFor      = 50 * time.Millisecond
		maxReadAttempts = uint64(2)
	)

	logConfig := commitlog.NewConfig()
	realLog, err := commitlog.NewLog(t.TempDir(), *logConfig)
	require.NoError(t, err)
	t.Cleanup(func() {
		require.NoError(t, realLog.Close())
	})

	ctx, cancel := context.WithTimeout(context.Background(), observeFor)
	t.Cleanup(cancel)

	commitLog := &countingCommitLog{Log: realLog}

	srv, err := newGRPCLogServer(&Config{
		CommitLog:  commitLog,
		Authorizer: allowAllAuthorizer{},
	})
	require.NoError(t, err)

	err = srv.ConsumeStream(&api.ConsumeRequest{Offset: 0}, consumeStreamStub{ctx: ctx})
	require.NoError(t, err)

	require.LessOrEqual(
		t,
		commitLog.reads.Load(),
		maxReadAttempts,
		"expected ConsumeStream to back off on ErrOffsetOutOfRange; busy loop detected",
	)
}

type allowAllAuthorizer struct{}

func (allowAllAuthorizer) Authorize(string, string, string) error {
	return nil
}

type countingCommitLog struct {
	*commitlog.Log
	reads atomic.Uint64
}

func (l *countingCommitLog) Read(offset uint64) (*api.Record, error) {
	l.reads.Add(1)
	return l.Log.Read(offset)
}

type consumeStreamStub struct {
	grpc.ServerStream
	ctx context.Context
}

func (s consumeStreamStub) Context() context.Context {
	return s.ctx
}

func (consumeStreamStub) Send(*api.ConsumeResponse) error {
	return nil
}
