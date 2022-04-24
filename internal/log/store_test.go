package log

import (
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/require"
)

var (
	recordData = []byte("hello world")
	recordLen  = uint64(len(recordData)) + recordLenNumBytes
)

func TestStoreAppendRead(t *testing.T) {
	f, err := ioutil.TempFile("", "store_append_read_test")
	require.NoError(t, err)
	defer os.Remove(f.Name())

	s, err := newStore(f)
	require.NoError(t, err)

	testAppend(t, s)
	testRead(t, s)
	testReadAt(t, s)

	s, err = newStore(f)
	require.NoError(t, err)
	testRead(t, s)
}

func testAppend(t *testing.T, s *store) {
	t.Helper()
	for i := uint64(1); i < 4; i++ {
		nn, pos, err := s.Append(recordData)
		require.NoError(t, err)
		// Starting byte position + num bytes written for the record
		// should equal the multiples of `recordLen`.
		require.Equal(t, pos+nn, recordLen*i)
	}
}

func testRead(t *testing.T, s *store) {
	t.Helper()
	var pos uint64
	for i := uint64(1); i < 4; i++ {
		read, err := s.Read(pos)
		require.NoError(t, err)
		require.Equal(t, recordData, read)
		pos += recordLen
	}
}
func testReadAt(t *testing.T, s *store) {
	t.Helper()
	var pos int64
	for i := uint64(1); i < 4; i++ {
		b := make([]byte, recordLenNumBytes)
		// Read recordLen/size into `b`.
		nn, err := s.ReadAt(b, pos)
		require.NoError(t, err)
		require.Equal(t, recordLenNumBytes, nn)
		pos += int64(nn)

		// Get the size of the record.
		size := enc.Uint64(b)
		// Read record data into `b`.
		b = make([]byte, size)
		nn, err = s.ReadAt(b, pos)
		require.NoError(t, err)
		require.Equal(t, recordData, b)
		require.Equal(t, int(size), nn)
		pos += int64(nn)
	}
}
