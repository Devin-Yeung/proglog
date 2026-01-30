package log

import (
	"fmt"
	"os"
	"path"

	api "github.com/Devin-Yeung/proglog/api/v1"
	"google.golang.org/protobuf/proto"
)

// segment represents a log segment, which consists of a store and an index.
type segment struct {
	// store is the store associated with this segment.
	store *store
	// index is the index associated with this segment.
	index *index
	// config holds the configuration for this segment.
	config Config
	// baseOffset is the starting point of this segment.
	baseOffset uint64
	// nextOffset is the right boundary (exclusive) of this segment.
	nextOffset uint64
}

// newSegment creates a new segment in the specified directory with the given base offset and configuration. The file
// names for the store and index are derived from the base offset. If index or store are missing, they will be created.
func newSegment(dir string, baseOffset uint64, config Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     config,
	}

	// handle the store
	storePath := path.Join(dir, fmt.Sprintf("%d.store", baseOffset))
	storeFile, err := os.OpenFile(storePath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	if s.store, err = newStore(storeFile); err != nil {
		return nil, err
	}

	// handle the index
	indexPath := path.Join(dir, fmt.Sprintf("%d.index", baseOffset))
	indexFile, err := os.OpenFile(indexPath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, err
	}

	if s.index, err = newIndex(indexFile, config); err != nil {
		return nil, err
	}

	// fetch the right boundary offset from the index
	if lastEntryOffset, _, err := s.index.Read(-1); err != nil {
		// todo: error is always EOF here?
		s.nextOffset = baseOffset // index is empty if eof
	} else {
		s.nextOffset = baseOffset + uint64(lastEntryOffset) + 1
	}

	return s, nil
}

// Append adds a new record to the segment and returns the offset of the appended record.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	cur := s.nextOffset
	record.Offset = cur

	// serialize the record
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	// append to the store
	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// append to the index
	relativeOffset := uint32(s.nextOffset - s.baseOffset)
	if err = s.index.Write(relativeOffset, pos); err != nil {
		return 0, err
	}

	s.nextOffset += 1
	return cur, nil
}

// Read retrieves a record from the segment at the specified **absolute** offset.
func (s *segment) Read(offset uint64) (*api.Record, error) {
	relativeOffset := int64(offset - s.baseOffset)

	// retrieve the position from the index
	_, pos, err := s.index.Read(relativeOffset)
	if err != nil {
		return nil, err
	}

	// read the record from the store
	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}

	// unmarshal the record
	record := &api.Record{}
	if err = proto.Unmarshal(p, record); err != nil {
		return nil, err
	}

	return record, nil
}

// Close closes the segment's store and index.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}

	if err := s.store.Close(); err != nil {
		return err
	}

	return nil
}

func (s *segment) IsFull() bool {
	return s.store.size >= s.config.segment.maxStoreBytes ||
		s.index.size >= s.config.segment.maxIndexBytes
}
