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

	log.Close()

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
