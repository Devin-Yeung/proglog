package log

import (
	"os"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"

	api "github.com/Devin-Yeung/proglog/api/v1"
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
		// todo: handle error or just skip invalid files?
		offset, _ := strconv.ParseUint(rawOffset, 10, 64)
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
