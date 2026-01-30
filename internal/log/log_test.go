package log

import (
	"fmt"
	"testing"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/docker/go-units"
	"github.com/stretchr/testify/require"
)

func TestLogIntegration(t *testing.T) {
	tempdir := t.TempDir()

	config := NewConfig().WithSegmentMaxStoreBytes(1 * units.MiB)

	log, err := NewLog(tempdir, *config)
	require.NoError(t, err)

	for i := 0; i < 1000; i++ {
		want := api.Record{Value: []byte(fmt.Sprintf("testing record %d", i))}
		// append record
		idx, err := log.Append(&want)
		require.NoError(t, err)
		require.Equal(t, idx, uint64(i))
		// read record
		got, err := log.Read(idx)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
		require.Equal(t, want.Offset, got.Offset)
	}

		require.NoError(t, log.Close())

	// reopen log
	log, err = NewLog(tempdir, *config)
	require.NoError(t, err)
	defer log.Close()

	// read records after reopening
	for i := 0; i < 1000; i++ {
		want := api.Record{Value: []byte(fmt.Sprintf("testing record %d", i)), Offset: uint64(i)}
		got, err := log.Read(uint64(i))
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
		require.Equal(t, want.Offset, got.Offset)
	}
}

func TestLogTruncate(t *testing.T) {
	tempdir := t.TempDir()
	config := NewConfig().WithSegmentMaxStoreBytes(100)

	log, err := NewLog(tempdir, *config)
	require.NoError(t, err)
	defer log.Close()

	// append records to create multiple segments
	for i := 0; i < 50; i++ {
		_, err := log.Append(&api.Record{Value: []byte(fmt.Sprintf("test data %d", i))})
		require.NoError(t, err)
	}

	// get the lowest and highest offsets
	lowest, err := log.LowestOffset()
	require.NoError(t, err)
	highest, err := log.HighestOffset()
	require.NoError(t, err)

	// choose a truncate point in the middle
	truncateOffset := lowest + (highest-lowest)/2

	// truncate
	err = log.Truncate(truncateOffset)
	require.NoError(t, err)

	// truncated offsets should return ErrOffsetOutOfRange
	_, err = log.Read(lowest)
	require.ErrorIs(t, err, ErrOffsetOutOfRange)

	// lowest offset should increase
	newLowest, err := log.LowestOffset()
	require.NoError(t, err)
	require.Greater(t, newLowest, lowest)

	// non-truncated offsets should still be readable
	record, err := log.Read(highest)
	require.NoError(t, err)
	require.NotNil(t, record)
}
