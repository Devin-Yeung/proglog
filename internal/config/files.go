package config

import (
	"os"
	"path/filepath"
)

var (
	CAFile         = configFile("ca.pem")
	ServerCertFile = configFile("server.pem")
	ServerKeyFile  = configFile("server-key.pem")
)

func configFile(filename string) string {
	if dir := os.Getenv("PROGLOG_CONFIG_DIR"); dir != "" {
		return filepath.Join(dir, filename)
	}
	panic("PROGLOG_CONFIG_DIR not set")
}
