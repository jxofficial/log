package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

var (
	offsetLenNumBytes uint64 = 4
	posLenNumBytes    uint64 = 8
	indexLenNumBytes  uint64 = offsetLenNumBytes + posLenNumBytes
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// Read takes in a record offset and returns the record's offset, its starting byte in the record store,
// and an error if any.
func (i *index) Read(in int64) (out uint32, pos uint64, err error) {
	if i.size == 0 {
		return 0, 0, io.EOF
	}
	// Return offset and pos of last record if `in` is -1
	if in == -1 {
		out = uint32((i.size / indexLenNumBytes) - 1)
	} else {
		out = uint32(in)
	}

	//indexEntryPos := uint64(out) * indexLenNumBytes

	return out, pos, nil
}

// Close synchronizes the data from the memory map back into the file, and closes the file.
func (i *index) Close() error {
	if err := i.mmap.Sync(gommap.MS_SYNC); err != nil {
		return err
	}
	if err := i.file.Sync(); err != nil {
		return err
	}
	// To get the offset of the last record, we look at the last index entry.
	// Hence, we truncate the file so that the last 12 bytes of the file correspond to the last index entry.
	if err := i.file.Truncate(int64(i.size)); err != nil {
		return err
	}
	return i.file.Close()
}

func newIndex(f *os.File, c Config) (*index, error) {
	idx := &index{
		file: f,
	}
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	idx.size = uint64(fi.Size())

	// Make the index file the max size and create a memory map from the index file.
	err = os.Truncate(f.Name(), int64(c.Segment.MaxIndexBytes))
	if err != nil {
		return nil, err
	}

	if idx.mmap, err = gommap.Map(
		idx.file.Fd(),
		gommap.PROT_READ|gommap.PROT_WRITE,
		gommap.MAP_SHARED,
	); err != nil {
		return nil, err
	}
	return idx, nil
}
