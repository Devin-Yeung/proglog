package server

import (
	"context"
	"fmt"
	"io"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/Devin-Yeung/proglog/internal/auth"
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
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

type grpcLogServer struct {
	api.UnimplementedLogServer
	*Config
}

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

type subjectCtxKey struct{}

func subject(ctx context.Context) string {
	val := ctx.Value(subjectCtxKey{})
	s, ok := val.(string)
	if !ok {
		return ""
	}
	return s
}

// authenticate is a gRPC interceptor that extracts the subject from the peer's TLS certificate and adds it to the context.
func authenticate(ctx context.Context) (context.Context, error) {
	p, ok := peer.FromContext(ctx)
	if !ok {
		return ctx, status.New(codes.Unknown, "could not find peer info").Err()
	}

	// make sure the downstream middleware can always read the subject from the context
	if p.AuthInfo == nil {
		return context.WithValue(ctx, subjectCtxKey{}, ""), nil
	}

	tlsInfo, ok := p.AuthInfo.(credentials.TLSInfo)
	if !ok {
		return ctx, status.New(codes.Unknown, "unexpected peer transport credentials").Err()
	}

	sbj := tlsInfo.State.VerifiedChains[0][0].Subject.CommonName
	ctx = context.WithValue(ctx, subjectCtxKey{}, sbj)
	return ctx, nil
}

func (s *grpcLogServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		produceAction,
	); err != nil {
		return nil, err
	}

	offset, err := s.CommitLog.Append(req.Record)
	if err != nil {
		return nil, err
	}
	return &api.ProduceResponse{Offset: offset}, nil
}

func (s *grpcLogServer) Consume(ctx context.Context, req *api.ConsumeRequest) (*api.ConsumeResponse, error) {
	if err := s.Authorizer.Authorize(
		subject(ctx),
		objectWildcard,
		consumeAction,
	); err != nil {
		return nil, err
	}

	record, err := s.CommitLog.Read(req.Offset)
	if err != nil {
		return nil, err
	}
	return &api.ConsumeResponse{Record: record}, nil
}

func (s *grpcLogServer) ConsumeStream(req *api.ConsumeRequest, stream api.Log_ConsumeStreamServer) error {
	cursor := req.Offset

	for {
		select {
		case <-stream.Context().Done():
			return nil
		default:
		}

		record, err := s.Consume(
			stream.Context(),
			&api.ConsumeRequest{Offset: cursor},
		)

		switch err.(type) {
		case nil:
			// ok
		case api.ErrOffsetOutOfRange:
			// block until the log notifies us of a new append, or the stream is canceled
			if err := s.CommitLog.WaitForAppend(stream.Context()); err != nil {
				return nil // context canceled
			}
			continue
		default:
			return err
		}

		if err := stream.Send(record); err != nil {
			return err
		}
		cursor += 1
	}
}

func (s *grpcLogServer) ProduceStream(stream api.Log_ProduceStreamServer) error {
	for {
		req, err := stream.Recv()
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}

		resp, err := s.Produce(stream.Context(), req)
		if err != nil {
			return err
		}

		if err := stream.Send(resp); err != nil {
			return err
		}
	}
}

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	opts = append(opts,
		// unary interceptor
		grpc.ChainUnaryInterceptor(
			grpc_auth.UnaryServerInterceptor(authenticate),
		),
		// streaming interceptors
		grpc.ChainStreamInterceptor(
			grpc_auth.StreamServerInterceptor(authenticate),
		),
	)

	gsrv := grpc.NewServer(opts...)
	// create the log service
	srv, err := newGRPCLogServer(config)
	if err != nil {
		return nil, err
	}
	// register the log service with the gRPC server
	api.RegisterLogServer(gsrv, srv)
	return gsrv, nil
}
