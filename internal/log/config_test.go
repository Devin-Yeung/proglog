package log

import (
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestDefaultConfig(t *testing.T) {
	c := NewConfig()

	require.Equal(t, c.segment.maxIndexBytes, uint64(1*units.MiB))
	require.Equal(t, c.segment.maxStoreBytes, uint64(1*units.MiB))
}

func TestNonDefaultConfig(t *testing.T) {
	c := NewConfig().
		WithSegmentMaxIndexBytes(10 * units.MiB).
		WithSegmentMaxStoreBytes(100 * units.MiB)

	require.Equal(t, c.segment.maxIndexBytes, uint64(10*units.MiB))
	require.Equal(t, c.segment.maxStoreBytes, uint64(100*units.MiB))
}
