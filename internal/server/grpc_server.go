package server

import (
	"github.com/Devin-Yeung/proglog/internal/server/middleware"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/auth"
	"github.com/grpc-ecosystem/go-grpc-middleware/v2/interceptors/logging"
	"go.uber.org/zap"
	"google.golang.org/grpc"

	api "github.com/Devin-Yeung/proglog/api/v1"
)

func NewGRPCServer(config *Config, opts ...grpc.ServerOption) (*grpc.Server, error) {
	// TODO: make the logger configurable
	logger, err := zap.NewDevelopment()
	if err != nil {
		return nil, err
	}

	loggerOpts := []logging.Option{
		logging.WithLogOnEvents(logging.StartCall, logging.FinishCall),
	}

	opts = append(opts,
		// unary interceptor
		grpc.ChainUnaryInterceptor(
			logging.UnaryServerInterceptor(middleware.ZapLogger(logger), loggerOpts...),
			auth.UnaryServerInterceptor(middleware.Authenticate),
		),
		// streaming interceptors
		grpc.ChainStreamInterceptor(
			logging.StreamServerInterceptor(middleware.ZapLogger(logger), loggerOpts...),
			auth.StreamServerInterceptor(middleware.Authenticate),
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
