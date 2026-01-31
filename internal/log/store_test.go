package log

import (
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var (
	dummyWrite = []byte("hello world")
	width      = uint64(len(dummyWrite)) + lenWidth
)

func TestStoreAppendRead(t *testing.T) {
	// create a temp file for testing
	f, err := os.CreateTemp("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	// create a new store
	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)

	// reopen the store
	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		n, pos, err := s.Append(dummyWrite)
		assert.NoError(t, err)
		assert.Equal(t, pos+n, width*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		assert.NoError(t, err)
		assert.Equal(t, dummyWrite, read)
		pos += width
	}
}

func TestStoreRemove(t *testing.T) {
	// create a temp file for testing
	f, err := os.CreateTemp("", "store_remove_test")
	require.NoError(t, err)

	// create a new store
	s, err := newStore(f)
	require.NoError(t, err)

	// write some data
	_, _, err = s.Append(dummyWrite)
	require.NoError(t, err)

	// get the file name before removing
	filename := s.File.Name()

	// remove the store
	err = s.Remove()
	require.NoError(t, err)

	// verify the file no longer exists
	_, err = os.Stat(filename)
	require.ErrorIs(t, err, os.ErrNotExist)

	// verify the store is closed - trying to read should fail
	_, err = s.Read(0)
	require.Error(t, err)
}
