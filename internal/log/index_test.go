package log

import (
	"os"
	"testing"

	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestIndex(t *testing.T) {
	config := NewConfig().WithSegmentMaxIndexBytes(10 * units.MiB)

	// create a temp file for testing
	f, err := os.CreateTemp(os.TempDir(), "index_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// create a new index
	index, err := newIndex(f, *config)
	require.NoError(t, err)
	defer index.Close()

	// no entries yet
	_, _, err = index.Read(-1)
	require.Error(t, err)

	// write some entries
	var entries = []struct {
		offset   uint32
		position uint64
	}{
		{offset: 0, position: 0},
		{offset: 1, position: 100},
		{offset: 2, position: 500},
	}

	for _, e := range entries {
		err = index.Write(e.offset, e.position)
		require.NoError(t, err)

		_, pos, err := index.Read(int64(e.offset))
		require.NoError(t, err)
		require.Equal(t, e.position, pos)
	}

	// read last entry
	lastOffset, lastPosition, err := index.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[len(entries)-1].offset, lastOffset)
	require.Equal(t, entries[len(entries)-1].position, lastPosition)

	// index out of bounds should give an error
	_, _, err = index.Read(int64(len(entries)))
	require.Error(t, err)

	// close and reopen the index
	err = index.Close()
	require.NoError(t, err)

	// the rebuild of the index should work without errors
	f, err = os.OpenFile(f.Name(), os.O_RDWR, 0600)
	require.NoError(t, err)

	index, err = newIndex(f, *config)
	require.NoError(t, err)
	defer index.Close()

	lastOffset, lastPosition, err = index.Read(-1)
	require.NoError(t, err)
	require.Equal(t, entries[len(entries)-1].offset, lastOffset)
	require.Equal(t, entries[len(entries)-1].position, lastPosition)

}

func TestIndexRemove(t *testing.T) {
	config := NewConfig().WithSegmentMaxIndexBytes(10 * units.MiB)

	// create a temp file for testing
	f, err := os.CreateTemp(os.TempDir(), "index_remove_test")
	require.NoError(t, err)

	// create a new index
	index, err := newIndex(f, *config)
	require.NoError(t, err)

	// write some entries
	err = index.Write(0, 0)
	require.NoError(t, err)

	// get the file name before removing
	filename := index.file.Name()

	// remove the index
	err = index.Remove()
	require.NoError(t, err)

	// verify the file no longer exists
	_, err = os.Stat(filename)
	require.Error(t, err)
	require.True(t, os.IsNotExist(err))
}
