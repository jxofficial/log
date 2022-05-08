package log

import (
	"github.com/golang/protobuf/proto"
	api "github.com/jxofficial/log/api/v1"
	"github.com/stretchr/testify/require"
	"io/ioutil"
	"os"
	"testing"
)

func TestLog(t *testing.T) {
	for scenario, fn := range map[string]func(t *testing.T, log *Log){
		"append and read a record succeeds":    testAppendRead,
		"read out of range returns an error":   testOutOfRangeErr,
		"log retains state after being closed": testInitExisting,
		"reader":                               testReader,
		"truncate":                             testTruncate,
	} {
		t.Run(scenario, func(t *testing.T) {
			dir, err := ioutil.TempDir("", "log_test")
			require.NoError(t, err)
			defer os.RemoveAll(dir)

			c := Config{}
			c.Segment.MaxStoreBytes = 32
			log, err := NewLog(dir, c)
			require.NoError(t, err)

			fn(t, log)
		})
	}
}

func testAppendRead(t *testing.T, log *Log) {
	r := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(r)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	recordFromLog, err := log.Read(offset)
	require.NoError(t, err)
	require.Equal(t, r.Value, recordFromLog.Value)
}

func testOutOfRangeErr(t *testing.T, log *Log) {
	r, err := log.Read(1)
	require.Nil(t, r)
	apiErr := err.(api.ErrOffsetOutOfRange)
	require.Equal(t, uint64(1), apiErr.Offset)
}

func testInitExisting(t *testing.T, existingLog *Log) {
	r := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := existingLog.Append(r)
		require.NoError(t, err)
	}
	require.NoError(t, existingLog.Close())

	lowest, err := existingLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowest)
	highest, err := existingLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highest)

	newLog, err := NewLog(existingLog.Dir, existingLog.Config)
	require.NoError(t, err)

	lowest, err = newLog.LowestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(0), lowest)
	highest, err = newLog.HighestOffset()
	require.NoError(t, err)
	require.Equal(t, uint64(2), highest)
}

func testReader(t *testing.T, log *Log) {
	r := &api.Record{
		Value: []byte("hello world"),
	}
	offset, err := log.Append(r)
	require.NoError(t, err)
	require.Equal(t, uint64(0), offset)

	reader := log.Reader()
	b, err := ioutil.ReadAll(reader)
	// Length of "hello world" in byte slice is 13, + recordLenNumBytes (8) = 21.
	require.Equal(t, 21, len(b))
	require.NoError(t, err)

	recordFromLog := &api.Record{}
	err = proto.Unmarshal(b[recordLenNumBytes:], recordFromLog)
	require.NoError(t, err)
	require.Equal(t, r.Value, recordFromLog.Value)
}

func testTruncate(t *testing.T, log *Log) {
	r := &api.Record{
		Value: []byte("hello world"),
	}
	for i := 0; i < 3; i++ {
		_, err := log.Append(r)
		require.NoError(t, err)
	}

	err := log.Truncate(1)
	require.NoError(t, err)

	// First segment contains records with offset 0 and 1.
	// Each record entry in the store should be 21 bytes:
	// 8 bytes to hold the len (13) and the record which is 13 bytes long.
	_, err = log.Read(1)
	require.Error(t, err)

	_, err = log.Read(2)
	require.NoError(t, err)
}
