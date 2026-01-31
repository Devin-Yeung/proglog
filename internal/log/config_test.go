package log

import (
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	c := NewConfig()

	assert.Equal(t, c.segment.maxIndexBytes, uint64(1*units.MiB))
	assert.Equal(t, c.segment.maxStoreBytes, uint64(1*units.MiB))
}

func TestNonDefaultConfig(t *testing.T) {
	c := NewConfig().
		WithSegmentMaxIndexBytes(10 * units.MiB).
		WithSegmentMaxStoreBytes(100 * units.MiB)

	assert.Equal(t, c.segment.maxIndexBytes, uint64(10*units.MiB))
	assert.Equal(t, c.segment.maxStoreBytes, uint64(100*units.MiB))
}
