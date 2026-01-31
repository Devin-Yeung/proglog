package log

import (
	"fmt"
	"testing"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append/read":             testAppendRead,
		"reopen":                  testReopen,
		"truncate":                testTruncate,
		"truncate active segment": testTruncateActive,
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

	n := uint64(50)

	// append records to create multiple segments
	for i := uint64(0); i < n; i++ {
		_, err := log.Append(&api.Record{Value: []byte("test data")})
		require.NoError(t, err)
	}

	// truncate
	err := log.Truncate(uint64(n / 2))
	require.NoError(t, err)

	// the size should be less than before
	size, err := log.Length()
	require.NoError(t, err)
	require.Less(t, size, n)

	// the first element can't be read
	_, err = log.Read(0)
	require.ErrorIs(t, err, ErrOffsetOutOfRange)

	// the last element can be read
	_, err = log.Read(n - 1)
	require.NoError(t, err)
}

func testTruncateActive(t *testing.T, log *Log) {
	defer func(log *Log) {
		err := log.Close()
		require.NoError(t, err)
	}(log)

	// append records to create multiple segments
	for i := 0; i < 50; i++ {
		_, err := log.Append(&api.Record{Value: []byte(fmt.Sprintf("test data %d", i))})
		require.NoError(t, err)
	}

	// can't truncate all records
	err := log.Truncate(50)
	require.ErrorIs(t, err, ErrSegmentActive)
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
		require.Equal(t, uint64(i), idx)
		// read record
		got, err := log.Read(idx)
		require.NoError(t, err)
		require.Equal(t, want.Value, got.Value)
		require.Equal(t, uint64(i), got.Offset)
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
