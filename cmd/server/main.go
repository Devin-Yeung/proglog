package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"syscall"

	"github.com/Devin-Yeung/proglog/internal/auth"
	commitlog "github.com/Devin-Yeung/proglog/internal/log"
	"github.com/Devin-Yeung/proglog/internal/server"
	"github.com/alecthomas/kong"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
)

func main() {
	cfg := Config{}
	kong.Parse(
		&cfg,
		kong.Name("proglog-server"),
		kong.Description("Run the proglog gRPC server."),
	)

	if err := run(cfg); err != nil {
		log.Fatal(err)
	}
}

func run(cfg Config) error {
	if cfg.ListenPort < 1 || cfg.ListenPort > 65535 {
		return fmt.Errorf("listen-port must be between 1 and 65535")
	}

	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return err
	}

	logCfg := commitlog.NewConfig()
	clog, err := commitlog.NewLog(cfg.DataDir, *logCfg)
	if err != nil {
		return err
	}
	defer clog.Close()

	authorizer, err := auth.NewAuthorizer(cfg.ACLModelFile, cfg.ACLPolicyFile)
	if err != nil {
		return err
	}

	tlsConfig, err := setupServerTLSConfig(cfg.CertFile, cfg.KeyFile, cfg.CAFile)
	if err != nil {
		return err
	}

	creds := credentials.NewTLS(tlsConfig)
	gRPCServer, err := server.NewGRPCServer(
		&server.Config{CommitLog: clog, Authorizer: authorizer},
		grpc.Creds(creds),
	)
	if err != nil {
		return err
	}

	listenAddr := net.JoinHostPort(cfg.ListenHost, strconv.Itoa(cfg.ListenPort))

	lis, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return err
	}
	defer lis.Close()

	errCh := make(chan error, 1)
	go func() {
		errCh <- gRPCServer.Serve(lis)
	}()

	log.Printf("gRPC server listening on %s", listenAddr)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(sigCh)

	select {
	case sig := <-sigCh:
		log.Printf("received %s, shutting down", sig)
		gRPCServer.GracefulStop()
		return nil
	case err := <-errCh:
		if err == nil || errors.Is(err, grpc.ErrServerStopped) {
			return nil
		}
		return err
	}
}

func setupServerTLSConfig(certFile string, keyFile string, caFile string) (*tls.Config, error) {
	cert, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}

	caPEM, err := os.ReadFile(caFile)
	if err != nil {
		return nil, err
	}

	caPool := x509.NewCertPool()
	if ok := caPool.AppendCertsFromPEM(caPEM); !ok {
		return nil, fmt.Errorf("failed to parse CA certificates from %s", filepath.Clean(caFile))
	}

	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{cert},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}, nil
}
