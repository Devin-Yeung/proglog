package log

import (
	"fmt"
	"math/rand"
	"os"
	"testing"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestSegment(t *testing.T) {
	tmpdir := t.TempDir()
	defer os.RemoveAll(tmpdir)

	c := NewConfig().WithSegmentMaxStoreBytes(10 * units.MiB)

	baseOffset := rand.Int()

	s, err := newSegment(tmpdir, uint64(baseOffset), *c)
	require.NoError(t, err)

	for i := 0; i < 10; i++ {
		want := &api.Record{Value: []byte(fmt.Sprintf("%d", baseOffset+i))}
		// append the record to the segment
		offset, err := s.Append(want)
		require.NoError(t, err)
		require.Equal(t, uint64(baseOffset+i), offset)

		// read the record back from the segment
		got, err := s.Read(offset)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}
	err = s.Close()
	require.NoError(t, err)

	// reopen the segment
	s, err = newSegment(tmpdir, uint64(baseOffset), *c)
	require.NoError(t, err)
	defer s.Close()

	// health check the reopened segment
	for i := 0; i < 10; i++ {
		want := &api.Record{Value: []byte(fmt.Sprintf("%d", baseOffset+i))}
		got, err := s.Read(uint64(baseOffset + i))
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
	}
}
