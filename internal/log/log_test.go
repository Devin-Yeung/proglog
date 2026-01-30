package log

import (
	"fmt"
	"testing"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append/read": testAppendRead,
		"reopen":      testReopen,
		"truncate":    testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			tempdir := t.TempDir()
			config := NewConfig().WithSegmentMaxStoreBytes(128)
			log, err := NewLog(tempdir, *config)
			require.NoError(t, err)
			// the lifetime of the log should be managed by each test
			fn(t, log)
		})
	}
}

func testTruncate(t *testing.T, log *Log) {
	defer func(log *Log) {
		err := log.Close()
		require.NoError(t, err)
	}(log)

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

func testAppendRead(t *testing.T, log *Log) {
	defer func(log *Log) {
		err := log.Close()
		require.NoError(t, err)
	}(log)

	for i := 0; i < 100; i++ {
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

	size, err := log.Length()
	require.NoError(t, err)
	require.Equal(t, uint64(100), size)
}

func testReopen(t *testing.T, log *Log) {
	// append records
	for i := 0; i < 100; i++ {
		want := api.Record{Value: []byte(fmt.Sprintf("testing record %d", i))}
		// append record
		_, err := log.Append(&want)
		require.NoError(t, err)
	}

	err := log.Close()
	require.NoError(t, err)

	// reopen log
	log, err = NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	size, err := log.Length()
	require.NoError(t, err)
	require.Equal(t, uint64(100), size)
}
