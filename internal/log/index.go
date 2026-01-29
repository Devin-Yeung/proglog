package log

import (
	"os"

	"github.com/docker/go-units"
	"github.com/tysonmote/gommap"
)

var (
	offsetWidth   = 4
	positionWidth = 8
	entryWidth    = offsetWidth + positionWidth
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
func newIndex(f *os.File) (*index, error) {
	idx := &index{
		file: f,
	}

	info, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}

	idx.size = uint64(info.Size())

	// preset the file size since mmap can't enlarge the file during the mapping
	// TODO: define the max index size via config
	var maxIndexBytes int64 = 10 * units.MiB
	if err = os.Truncate(f.Name(), maxIndexBytes); err != nil {
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

// Close ensures that all changes to the memory-mapped file are synchronized
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
	return nil
}
