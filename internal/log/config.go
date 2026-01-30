package log

import "github.com/docker/go-units"

type Config struct {
	segment struct {
		maxStoreBytes uint64
		maxIndexBytes uint64
		initialOffset uint64
	}
}

func NewConfig() *Config {
	config := &Config{}
	config.segment.maxStoreBytes = 1 * units.MiB
	config.segment.maxIndexBytes = 1 * units.MiB
	config.segment.initialOffset = 0
	return config
}

func (c *Config) WithSegmentMaxStoreBytes(bytes uint64) *Config {
	if bytes == 0 {
		bytes = 1 * units.MiB
	}
	c.segment.maxStoreBytes = bytes
	return c
}

func (c *Config) WithSegmentMaxIndexBytes(bytes uint64) *Config {
	if bytes == 0 {
		bytes = 1 * units.MiB
	}
	c.segment.maxIndexBytes = bytes
	return c
}

func (c *Config) WithSegmentInitialOffset(offset uint64) *Config {
	c.segment.initialOffset = offset
	return c
}
