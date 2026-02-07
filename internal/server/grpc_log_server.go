package server

import (
	"context"
	"io"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/Devin-Yeung/proglog/internal/server/middleware"
)

type grpcLogServer struct {
	api.UnimplementedLogServer
	*Config
}

func (s *grpcLogServer) Produce(ctx context.Context, req *api.ProduceRequest) (*api.ProduceResponse, error) {
	if err := s.Authorizer.Authorize(
		middleware.Subject(ctx),
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
		middleware.Subject(ctx),
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
