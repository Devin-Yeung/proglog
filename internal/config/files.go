package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
	RootCertFile   = configFile("root-client.pem")
	RootKeyFile    = configFile("root-client-key.pem")
	NobodyCertFile = configFile("nobody-client.pem")
	NobodyKeyFile  = configFile("nobody-client-key.pem")
)

func configFile(filename string) string {
	if dir := os.Getenv("PROGLOG_CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	panic("PROGLOG_CONFIG_DIR not set")
}
