package server

import (
	grpc_auth "github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"google.golang.org/grpc"

	api "github.com/Devin-Yeung/proglog/api/v1"
)

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
