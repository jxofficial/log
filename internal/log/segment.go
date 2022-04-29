package log

import (
	"fmt"
	"github.com/golang/protobuf/proto"
	"os"
	"path"

	api "github.com/jxofficial/log/api/v1"
)

type segment struct {
	store *store
	index *index
	// baseOffset is the offset of the first record in the segment.
	// nextOffset is the offset of the next record to be added to the segment.
	baseOffset, nextOffset uint64
	config                 Config
}

func newSegment(dir string, baseOffset uint64, c Config) (*segment, error) {
	s := &segment{
		baseOffset: baseOffset,
		config:     c,
	}
	var err error

	// Set up store file.
	storeFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".store")),
		os.O_RDWR|os.O_CREATE|os.O_APPEND,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.store, err = newStore(storeFile)
	if err != nil {
		return nil, err
	}

	// Set up index file.
	indexFile, err := os.OpenFile(
		path.Join(dir, fmt.Sprintf("%d%s", baseOffset, ".index")),
		os.O_RDWR|os.O_CREATE,
		0644,
	)
	if err != nil {
		return nil, err
	}
	s.index, err = newIndex(indexFile, c)
	if err != nil {
		return nil, err
	}

	relativeOffset, _, err := s.index.Read(-1)
	// If there is an error reading the last entry in the index, it means the index file is empty.
	// Hence, the offset of the next record to be added
	// is the offset of the first record in the segment, which is `baseOffset`.
	if err != nil {
		s.nextOffset = baseOffset
	} else {
		// e.g. the segment holds records from offset 10 onwards,
		// and the index file already references 2 records (`relativeOffset` is 1)
		// Hence, the offset of the next record to be added to the segment is
		// 10 + 1 + 1 = 12 (the 13th record).
		s.nextOffset = baseOffset + uint64(relativeOffset) + 1
	}

	return s, nil
}

// Append appends the record into the segment and returns the record's offset, and error if any.
func (s *segment) Append(record *api.Record) (offset uint64, err error) {
	currOffset := s.nextOffset
	record.Offset = currOffset
	p, err := proto.Marshal(record)
	if err != nil {
		return 0, err
	}

	_, pos, err := s.store.Append(p)
	if err != nil {
		return 0, err
	}

	// index offsets are relative
	err = s.index.Write(uint32(s.nextOffset-s.baseOffset), pos)
	if err != nil {
		return 0, err
	}

	s.nextOffset++
	return currOffset, nil
}

// Read takes in a record offset and returns the corresponding record.
func (s *segment) Read(off uint64) (*api.Record, error) {
	_, pos, err := s.index.Read(int64(off - s.baseOffset))
	if err != nil {
		return nil, err
	}

	p, err := s.store.Read(pos)
	if err != nil {
		return nil, err
	}
	record := &api.Record{}
	err = proto.Unmarshal(p, record)
	return record, err
}

func (s *segment) IsMaxed() bool {
	return s.store.size >= s.config.Segment.MaxStoreBytes ||
		s.index.size >= s.config.Segment.MaxIndexBytes
}

// Remove closes and removes the segment's store and index files.
func (s *segment) Remove() error {
	if err := s.Close(); err != nil {
		return err
	}
	if err := os.Remove(s.index.Name()); err != nil {
		return err
	}
	if err := os.Remove(s.store.Name()); err != nil {
		return err
	}
	return nil
}

// Close closes the segment's store and index files.
func (s *segment) Close() error {
	if err := s.index.Close(); err != nil {
		return err
	}
	if err := s.store.Close(); err != nil {
		return err
	}
	return nil
}

// nearestMultiple returns the nearest and lesser multiple of k in j.
// e.g. nearestMultiple(9, 4) returns 8.
func nearestMultiple(j, k uint64) uint64 {
	return (j / k) * k
}
