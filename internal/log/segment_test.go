package log

import (
	"fmt"
	"math/rand"
	"os"
	"path"
	"testing"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/docker/go-units"
	"github.com/stretchr/testify/assert"
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
		assert.NoError(t, err)
		assert.Equal(t, uint64(baseOffset+i), offset)

		// read the record back from the segment
		got, err := s.Read(offset)
		assert.NoError(t, err)
		assert.Equal(t, want.Value, got.Value)
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
		assert.NoError(t, err)
		assert.Equal(t, want.Value, got.Value)
	}
}

func TestSegmentRemove(t *testing.T) {
	tmpdir := t.TempDir()

	c := NewConfig().WithSegmentMaxStoreBytes(10 * units.MiB)

	baseOffset := rand.Int()

	s, err := newSegment(tmpdir, uint64(baseOffset), *c)
	require.NoError(t, err)

	// append some records
	for i := 0; i < 3; i++ {
		want := &api.Record{Value: []byte(fmt.Sprintf("%d", baseOffset+i))}
		_, err := s.Append(want)
		require.NoError(t, err)
	}

	// get the file paths before removing
	storePath := path.Join(tmpdir, fmt.Sprintf("%d.store", baseOffset))
	indexPath := path.Join(tmpdir, fmt.Sprintf("%d.index", baseOffset))

	// verify files exist before removal
	_, err = os.Stat(storePath)
	require.NoError(t, err)
	_, err = os.Stat(indexPath)
	require.NoError(t, err)

	// remove the segment
	err = s.Remove()
	require.NoError(t, err)

	// verify both files no longer exist
	_, err = os.Stat(storePath)
	require.ErrorIs(t, err, os.ErrNotExist)

	_, err = os.Stat(indexPath)
	require.ErrorIs(t, err, os.ErrNotExist)
}
