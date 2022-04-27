package log

import (
	"io"
	"os"

	"github.com/tysonmote/gommap"
)

const (
	offsetLenNumBytes uint64 = 4
	posLenNumBytes    uint64 = 8
	indexLenNumBytes  uint64 = offsetLenNumBytes + posLenNumBytes
)

type index struct {
	file *os.File
	mmap gommap.MMap
	size uint64
}

// Read takes in a record offset and returns the record's offset,
// its starting byte in the record store, and an error if any.
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

	indexEntryPos := uint64(out) * indexLenNumBytes
	if i.size < indexEntryPos+indexLenNumBytes {
		return 0, 0, io.EOF
	}
	// record offset
	out = enc.Uint32(i.mmap[indexEntryPos : indexEntryPos+offsetLenNumBytes])
	// byte the record starts at in the store
	pos = enc.Uint64(i.mmap[indexEntryPos+offsetLenNumBytes : indexEntryPos+indexLenNumBytes])
	return out, pos, nil
}

// Write writes an index entry into the index.
func (i *index) Write(off uint32, pos uint64) error {
	// Check if mmap has enough space to add a new index entry.
	if uint64(len(i.mmap)) < i.size+indexLenNumBytes {
		return io.EOF
	}
	enc.PutUint32(i.mmap[i.size:i.size+offsetLenNumBytes], off)
	enc.PutUint64(i.mmap[i.size+offsetLenNumBytes:i.size+indexLenNumBytes], pos)
	i.size += indexLenNumBytes
	return nil
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

func (i *index) Name() string {
	return i.file.Name()
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
