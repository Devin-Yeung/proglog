package server

import (
	"context"
	"fmt"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/Devin-Yeung/proglog/internal/auth"
)

const (
	objectWildcard = "*"
	produceAction  = "produce"
	consumeAction  = "consume"
)

type CommitLog interface {
	// Append adds a record to the log and returns its offset.
	Append(record *api.Record) (uint64, error)
	// Read retrieves a record from the log at the specified offset.
	Read(offset uint64) (*api.Record, error)
	// WaitForAppend blocks until a new record is appended or the context is canceled.
	WaitForAppend(ctx context.Context) error
}

type Authorizer interface {
	// Authorize checks if the subject is allowed to perform the action on the object.
	Authorize(subject, object, action string) error
}

type Config struct {
	CommitLog  CommitLog
	Authorizer Authorizer
}

// interface compliance check
var _ api.LogServer = (*grpcLogServer)(nil)
var _ Authorizer = (*auth.Authorizer)(nil)

func newGRPCLogServer(config *Config) (*grpcLogServer, error) {
	if config == nil {
		return nil, fmt.Errorf("missing server config")
	}

	if config.CommitLog == nil || config.Authorizer == nil {
		return nil, fmt.Errorf("incomplete server config")
	}

	return &grpcLogServer{
		Config: config,
	}, nil
}
