package log

import (
	"fmt"
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/Devin-Yeung/proglog/api/v1"
)

var (
	ErrOffsetOutOfRange = fmt.Errorf("offset out of range")
	ErrSegmentActive    = fmt.Errorf("cannot truncate active segment")
)

type Log struct {
	Dir    string
	Config Config
	mu     sync.RWMutex
	// current active segment for appending new records
	activeSegment *segment
	// all segments, including active and inactive ones
	segments []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	l := &Log{
		Dir:    dir,
		Config: c,
	}

	if err := l.setup(); err != nil {
		return nil, err
	}

	return l, nil
}

func (l *Log) setup() error {
	files, err := os.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	// retrieve base offsets from segment files
	var baseOffsets []uint64

	for _, file := range files {
		if file.IsDir() {
			continue
		}

		rawOffset := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		offset, err := strconv.ParseUint(rawOffset, 10, 64)
		if err != nil {
			continue // skip files with invalid names
		}
		baseOffsets = append(baseOffsets, offset)
	}

	// deduplicate and sort
	baseOffsets = tidyOffsets(baseOffsets)

	for _, offset := range baseOffsets {
		if err := l.newSegment(offset); err != nil {
			return err
		}
	}
	// if no segments exist, create the initial segment
	if len(l.segments) == 0 {
		if err := l.newSegment(l.Config.segment.initialOffset); err != nil {
			return err
		}
	}
	return nil
}

// newSegment creates a new segment and sets it as the active segment.
func (l *Log) newSegment(baseOffset uint64) error {
	s, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}

	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

// Append adds a new record to the log and returns its index
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	offset, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}

	// check if active segment is full
	if l.activeSegment.IsFull() {
		err = l.newSegment(l.activeSegment.nextOffset)
		if err != nil {
			return 0, err
		}
	}
	return offset, nil
}

// Read retrieves a record by its offset from the log.
func (l *Log) Read(offset uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	// find the segment that contains the offset
	var s *segment
	for _, seg := range l.segments {
		if seg.baseOffset <= offset && offset < seg.nextOffset {
			s = seg
			break
		}
	}

	if s == nil {
		return nil, ErrOffsetOutOfRange
	}

	return s.Read(offset)
}

// Close closes all segments in the log.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()

	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Truncate removes all segments with base offsets lower than the specified lowest offset.
// If caller try to truncate the active segment, an error will be returned.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()

	if lowest >= l.activeSegment.nextOffset {
		return ErrSegmentActive
	}

	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset-1 < lowest {
			if err := s.Remove(); err != nil {
				return err
			}
		} else {
			segments = append(segments, s)
		}
	}
	l.segments = segments
	return nil
}

// LowestOffset returns the lowest offset in the log.
// The api is reserved for distributed log use cases.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// Invariant: l.segments is never empty after successful initialization.
	if len(l.segments) == 0 {
		panic("segments list should never be empty")
	}
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the highest offset in the log.
// The api is reserved for distributed log use cases.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	// Invariant: l.segments is never empty after successful initialization.
	if len(l.segments) == 0 {
		panic("segments list should never be empty")
	}
	// right boundary (exclusive)
	offset := l.segments[len(l.segments)-1].nextOffset

	if offset == 0 {
		return 0, ErrOffsetOutOfRange
	}

	return offset - 1, nil
}

// Length returns the number of records in the log.
func (l *Log) Length() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()

	low := l.segments[0].baseOffset
	high := l.segments[len(l.segments)-1].nextOffset - 1

	return high - low + 1, nil
}

// tidyOffsets removes duplicates from the offsets slice and return a sorted slice of unique offsets.
func tidyOffsets(offsets []uint64) []uint64 {
	seen := make(map[uint64]struct{})
	var result []uint64
	for _, offset := range offsets {
		if _, ok := seen[offset]; !ok {
			seen[offset] = struct{}{}
			result = append(result, offset)
		}
	}
	sort.Slice(result, func(i, j int) bool { return result[i] < result[j] })
	return result
}
