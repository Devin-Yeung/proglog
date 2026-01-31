package server

import (
	"context"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"google.golang.org/grpc"
)

type CommitLog interface {
	// Append adds a record to the log and returns its offset.
	Append(record *api.Record) (uint64, error)
	// Read retrieves a record from the log at the specified offset.
	Read(offset uint64) (*api.Record, error)
}

type Config struct {
	CommitLog
}

// interface compliance check
var _ api.LogServer = (*grpcLogServer)(nil)

type grpcLogServer struct {
	api.UnimplementedLogServer
	*Config
}

func newGRPCLogServer(config *Config) (*grpcLogServer, error) {
	srv := &grpcLogServer{
		Config: config,
	}
	return srv, nil
}

func (s *grpcLogServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcLogServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func NewGRPCServer(config *Config) (*grpc.Server, error) {
	gsrv := grpc.NewServer()
	// create the log service
	srv, err := newGRPCLogServer(config)
	if err != nil {
		return nil, err
	}
	// register the log service with the gRPC server
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
