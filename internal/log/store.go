package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	byteOrder = binary.LittleEndian
)

const (
	// lenWidth is the number of bytes used to store the length of each record.
	lenWidth = 8
)

// store is how we persist our log records to disk.
type store struct {
	// File is the underlying file handle used for persistence.
	*os.File
	// mu guards concurrent access to the file, buffer, and size.
	mu sync.Mutex
	// buf buffers writes to reduce syscalls and improve performance.
	buf *bufio.Writer
	// size is the current number of bytes written to the store.
	size uint64
}

func newStore(f *os.File) (*store, error) {
	info, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := uint64(info.Size())

	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append writes p to the store and returns the number of bytes written, the position
// +----------------------------+--------------+
// | length of record (8 bytes) | record bytes |
// +----------------------------+--------------+
func (s *store) Append(p []byte) (n uint64, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	pos = s.size
	// encoding the length of the record
	if err := binary.Write(s.buf, byteOrder, uint64(len(p))); err != nil {
		return 0, 0, err
	}

	// writing the record itself
	nn, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	nn += lenWidth
	s.size += uint64(nn)

	return uint64(nn), pos, nil
}

// Read reads a record from the store at the given position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to ensure all data is written to the file
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}

	// Read the length of the record
	sizeBuf := make([]byte, lenWidth)
	if _, err := s.File.ReadAt(sizeBuf, int64(pos)); err != nil {
		return nil, err
	}

	// Read the record itself
	b := make([]byte, byteOrder.Uint64(sizeBuf))
	if _, err := s.File.ReadAt(b, int64(pos+lenWidth)); err != nil {
		return nil, err
	}

	return b, nil
}

// ReadAt reads len(p) bytes from the store at the given offset.
func (s *store) ReadAt(p []byte, off int64) (n int, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Flush the buffer to ensure all data is written to the file
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}

	return s.File.ReadAt(p, off)
}

// Close flushes the buffer and closes the underlying file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	err := s.buf.Flush()
	if err != nil {
		return err
	}

	return s.File.Close()
}

// Remove closes the store and removes the underlying file from disk.
func (s *store) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	return os.Remove(s.File.Name())
}
