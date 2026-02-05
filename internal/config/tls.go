package config

import (
	"crypto/tls"
	"crypto/x509"
	"os"
)

type TLSConfig struct {
	CertFile      string
	KeyFile       string
	CAFile        string
	ServerAddress string
	// Is this config for a server
	Server bool
}

// SetupTLSConfig sets up a tls.Config based on the role (server/client)
func SetupTLSConfig(c TLSConfig) (*tls.Config, error) {
	tlsConfig := &tls.Config{}

	// if cert and key files are provided, load them
	if c.CertFile != "" && c.KeyFile != "" {
		cert, err := tls.LoadX509KeyPair(c.CertFile, c.KeyFile)
		if err != nil {
			return nil, err
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
	}

	// if custom ca is provided, load it
	if c.CAFile != "" {
		b, err := os.ReadFile(c.CAFile)
		if err != nil {
			return nil, err
		}
		certPool := x509.NewCertPool()
		// our private CA is the only trusted CA
		ok := certPool.AppendCertsFromPEM(b)
		if !ok {
			return nil, err
		}

		if c.Server {
			tlsConfig.ClientCAs = certPool
			// using mtls: we want to verify client certs
			tlsConfig.ClientAuth = tls.RequireAndVerifyClientCert
		} else {
			tlsConfig.RootCAs = certPool
		}
		tlsConfig.ServerName = c.ServerAddress
	}
	return tlsConfig, nil
}
