package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetWidth   uint64 = 4
	positionWidth uint64 = 8
	entryWidth           = offsetWidth + positionWidth
)

// Offset: the offset of current record relative to the segment's base offset
// Position: the *absolute position* of current record in the store file
//
// 0          4 bytes                     12 bytes
// +----------+---------------------------+
// |  Offset  |         Position          |
// +----------+---------------------------+
// |  uint32  |          uint64           |
// +----------+---------------------------+
// |<-- 4B -->|<----------- 8B ---------->|
// |<-------- entryWidth (12B) ---------->|
type index struct {
	// file is the underlying file handle used for the index.
	file *os.File
	// mmap is the memory-mapped representation of the index file.
	mmap gommap.MMap
	// size is the actual size of the index in bytes and tells us where to write the next entry.
	size uint64
}

// newIndex creates a new index for the given file.
func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}

	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(info.Size())

	// preset the file size since mmap can't enlarge the file during the mapping
	if err = os.Truncate(f.Name(), int64(c.segment.maxIndexBytes)); err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE, // grant read and write permissions
		gommap.MAP_SHARED,                  // changes will be shared with other processes
	); err != nil {
		return nil, err
	}

	return idx, nil
}

// Close ensures that all changes to the memory-mapped file are synchronized and releases all resources.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// Truncate the file to the size of the index to remove any unused space.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	if err := i.mmap.UnsafeUnmap(); err != nil {
		return err
	}
	return i.file.Close()
}

// Read takes an index and returns the corresponding de-surged offset and absolute position in the store file.
// If idx is -1, it reads the last entry in the index and returns its actually offset.
// The type of index is int64 on purpose to allow -1 as a special value and cover the full range of uint32.
func (i *index) Read(idx int64) (uint32, uint64, error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}

	// if idx is -1, read the last entry
	if idx == -1 {
		idx = int64((i.size / entryWidth) - 1)
	}

	idxLocation := uint64(idx) * entryWidth
	if i.size < idxLocation+entryWidth {
		return 0, 0, io.EOF
	}

	offset := byteOrder.Uint32(i.mmap[idxLocation : idxLocation+offsetWidth])
	position := byteOrder.Uint64(i.mmap[idxLocation+offsetWidth : idxLocation+entryWidth])
	return offset, position, nil
}

// Write appends a new offset and position entry to the index.
func (i *index) Write(offset uint32, pos uint64) error {
	// check if there is enough space to write a new entry
	if uint64(len(i.mmap)) < i.size+entryWidth {
		return io.EOF
	}

	// encode offset
	byteOrder.PutUint32(i.mmap[i.size:i.size+offsetWidth], offset)
	// encode position
	byteOrder.PutUint64(i.mmap[i.size+offsetWidth:i.size+entryWidth], pos)

	i.size += entryWidth

	return nil
}

// Remove closes the index and removes the underlying file from disk.
func (i *index) Remove() error {
	if err := i.Close(); err != nil {
		return err
	}
	return os.Remove(i.file.Name())
}
