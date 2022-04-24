package log

import (
	"bufio"
	"encoding/binary"
	"os"
	"sync"
)

var (
	enc = binary.BigEndian
)

const (
	recordLenNumBytes = 8
)

type store struct {
	*os.File
	mu   sync.Mutex
	buf  *bufio.Writer
	size uint64
}

func newStore(f *os.File) (*store, error) {
	fi, err := os.Stat(f.Name())
	if err != nil {
		return nil, err
	}
	size := uint64(fi.Size())
	return &store{
		File: f,
		size: size,
		buf:  bufio.NewWriter(f),
	}, nil
}

// Append appends a record (p) into the store. It returns the number of bytes written,
// and the starting byte position of the record entry in the store. It also returns an error if any.
func (s *store) Append(p []byte) (nn, pos uint64, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	pos = s.size
	// Write the length of the record (represented in big endian encoding)
	// into the store's buffered writer.
	if err := binary.Write(s.buf, enc, uint64(len(p))); err != nil {
		return 0, 0, err
	}
	numBytesWritten, err := s.buf.Write(p)
	if err != nil {
		return 0, 0, err
	}

	numBytesWritten += recordLenNumBytes
	s.size += uint64(numBytesWritten)
	return uint64(numBytesWritten), pos, nil
}

// Read returns the record data at the specified position.
func (s *store) Read(pos uint64) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	// Flush the buffer to ensure all data has been flushed into the file.
	if err := s.buf.Flush(); err != nil {
		return nil, err
	}
	// Get the record size, represented in big endian encoding.
	size := make([]byte, recordLenNumBytes)
	if _, err := s.File.ReadAt(size, int64(pos)); err != nil {
		return nil, err
	}
	// Make a byte slice of the correct size to hold the record data.
	b := make([]byte, enc.Uint64(size))
	if _, err := s.File.ReadAt(b, int64(pos+recordLenNumBytes)); err != nil {
		return nil, err
	}
	return b, nil
}

// ReadAt reads the record data for the given offset into `p`.
func (s *store) ReadAt(p []byte, off int64) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.buf.Flush(); err != nil {
		return 0, err
	}
	return s.File.ReadAt(p, off)
}

// Close flushes the buffer and closes the file.
func (s *store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	err := s.buf.Flush()
	if err != nil {
		return err
	}
	return s.File.Close()
}
