package server

import (
	"context"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

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
