package log

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLog(t *testing.T) {
	defaultConfig := func() *Config {
		return NewConfig().WithSegmentMaxStoreBytes(128)
	}

	type testCase struct {
		name string
		fn   func(t *testing.T, log *Log)
		cfg  *Config
	}

	configFor := func(cfg *Config) *Config {
		if cfg == nil {
			return defaultConfig()
		}
		return cfg
	}

	for _, tc := range []testCase{
		{name: "append/read", fn: testAppendRead},
		{name: "reopen", fn: testReopen},
		{name: "truncate", fn: testTruncate},
		{name: "truncate active segment", fn: testTruncateActive},
		{
			name: "concurrent writes",
			fn:   testConcurrentWrites,
			cfg: NewConfig().
				WithSegmentMaxStoreBytes(4 * 1024 * 1024).
				WithSegmentMaxIndexBytes(4 * 1024 * 1024),
		},
		{name: "wait for append", fn: testWaitForAppend},
		{name: "wait for append context canceled", fn: testWaitForAppendContextCanceled},
	} {
		t.Run(tc.name, func(t *testing.T) {
			tempdir := t.TempDir()
			config := configFor(tc.cfg)
			log, err := NewLog(tempdir, *config)
			require.NoError(t, err)
			// the lifetime of the log should be managed by each test
			tc.fn(t, log)
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
		assert.NoError(t, err)
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
	require.ErrorIs(t, err, api.ErrOffsetOutOfRange{Offset: 0})

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
		assert.NoError(t, err)
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
		assert.NoError(t, err)
		assert.Equal(t, uint64(i), idx)
		// read record
		got, err := log.Read(idx)
		assert.NoError(t, err)
		assert.Equal(t, want.Value, got.Value)
		assert.Equal(t, uint64(i), got.Offset)
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
		assert.NoError(t, err)
	}

	err := log.Close()
	require.NoError(t, err)

	// reopen log
	log, err = NewLog(log.Dir, log.Config)
	require.NoError(t, err)

	size, err := log.Length()
	require.NoError(t, err)
	require.Equal(t, uint64(100), size)

	err = log.Close()
	require.NoError(t, err)
}

func testWaitForAppend(t *testing.T, log *Log) {
	defer func(log *Log) {
		err := log.Close()
		require.NoError(t, err)
	}(log)

	ctx := context.Background()
	done := make(chan struct{})

	go func() {
		err := log.WaitForAppend(ctx)
		assert.NoError(t, err)
		close(done)
	}()

	// WaitForAppend should be blocking, not returning immediately
	select {
	case <-done:
		t.Fatal("WaitForAppend returned before any record was appended")
	case <-time.After(50 * time.Millisecond):
		// expected: still blocking
	}

	// now append a record to unblock the waiter
	_, err := log.Append(&api.Record{Value: []byte("wake up")})
	require.NoError(t, err)

	select {
	case <-done:
		// expected: WaitForAppend returned
	case <-time.After(time.Second):
		t.Fatal("WaitForAppend did not return after Append")
	}
}

func testWaitForAppendContextCanceled(t *testing.T, log *Log) {
	defer func(log *Log) {
		err := log.Close()
		require.NoError(t, err)
	}(log)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- log.WaitForAppend(ctx)
	}()

	// should be blocking
	select {
	case <-done:
		t.Fatal("WaitForAppend returned before context was canceled")
	case <-time.After(50 * time.Millisecond):
		// expected
	}

	cancel()

	select {
	case err := <-done:
		require.ErrorIs(t, err, context.Canceled)
	case <-time.After(time.Second):
		t.Fatal("WaitForAppend did not return after context cancel")
	}
}

func testConcurrentWrites(t *testing.T, log *Log) {
	defer func(log *Log) {
		err := log.Close()
		require.NoError(t, err)
	}(log)

	wg := sync.WaitGroup{}

	worker := func() {
		defer wg.Done()
		for i := 0; i < 1000; i++ {
			_, err := log.Append(&api.Record{Value: []byte("test data")})
			assert.NoError(t, err)
		}
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go worker()
	}

	wg.Wait()

	size, err := log.Length()
	require.NoError(t, err)
	require.Equal(t, uint64(10000), size)
}
