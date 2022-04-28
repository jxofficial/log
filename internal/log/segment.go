package log

import (
	"fmt"
	"os"
	"path"
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
	// If there is an error, it means the index file is empty.
	// Hence, the offset of the next record to be added
	// is the offset of the first record in the segment, which is `baseOffset`.
	if err != nil {
		s.nextOffset = baseOffset
	} else {
		// e.g. the segment holds records from offset 10 onwards,
		// and the index file already references 2 records (`relativeOffset is 1`)
		// Hence, the offset of the next record to be added to the segment is
		// 10 + 1 + 1 = 12 (the 13th record).
		s.nextOffset = baseOffset + uint64(relativeOffset) + 1
	}

	return s, nil
}
