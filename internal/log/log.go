package log

import (
	"fmt"
	api "github.com/jxofficial/log/api/v1"
	"io/ioutil"
	"path"
	"sort"
	"strconv"
	"strings"
	"sync"
)

type Log struct {
	Dir string
	Config

	mu            sync.RWMutex
	activeSegment *segment
	segments      []*segment
}

func NewLog(dir string, c Config) (*Log, error) {
	if c.Segment.MaxStoreBytes == 0 {
		c.Segment.MaxStoreBytes = 1024
	}
	if c.Segment.MaxIndexBytes == 0 {
		c.Segment.MaxIndexBytes = 1024
	}
	l := &Log{
		Dir:    dir,
		Config: c,
	}
	return l, l.setup()
}

func (l *Log) setup() error {
	// Read all store and index files in the directory.
	files, err := ioutil.ReadDir(l.Dir)
	if err != nil {
		return err
	}
	var baseOffsets []uint64
	for _, file := range files {
		// each store and index file is prefixed with the offset of the first entry in the file.
		// e.g. 30.store means the file holds records starting from offset 30.
		offsetStr := strings.TrimSuffix(file.Name(), path.Ext(file.Name()))
		// bitSize 0 means the value will fit into an `int`.
		offset, _ := strconv.ParseUint(offsetStr, 10, 0)
		baseOffsets = append(baseOffsets, offset)
	}

	sort.Slice(baseOffsets, func(i, j int) bool {
		return baseOffsets[i] < baseOffsets[j]
	})

	for i := 0; i < len(baseOffsets); i++ {
		if err = l.newSegment(baseOffsets[i]); err != nil {
			return err
		}
		// `baseOffsets` slice contains duplicate offset as it includes store and index.
		// Ignore the duplicate offset.
		i++
	}

	// No store or index files in the log.
	if l.segments == nil {
		if err = l.newSegment(l.Config.Segment.InitialOffset); err != nil {
			return err
		}
	}
	return nil
}

// Append appends a record to the log, and returns the record's offset.
func (l *Log) Append(record *api.Record) (uint64, error) {
	l.mu.Lock()
	defer l.mu.Unlock()

	off, err := l.activeSegment.Append(record)
	if err != nil {
		return 0, err
	}
	if l.activeSegment.IsMaxed() {
		// create a new segment for the log, and set it as the active segment.
		err = l.newSegment(off + 1)
	}
	return off, err
}

func (l *Log) Read(offset uint64) (*api.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	var segment *segment
	// Find the segment which houses the record having the provided offset.
	for _, s := range l.segments {
		// We need the second condition because we cannot just take the first occurrence of offset > baseOffset.
		// The first segment could hold offsets of 0 - 10, and if the offset is 15,
		// then the record is actually found in the second segment.
		if offset >= s.baseOffset && offset < s.nextOffset {
			segment = s
			break
		}
	}
	// Second condition is technically not needed as `segment` will already be nil
	// if you pass in an offset like 10000000,
	// as it will not satisfy the condition of offset < s.nextOffset in the for loop.
	if segment == nil || offset >= segment.nextOffset {
		return nil, fmt.Errorf("offset out of range: %d", offset)
	}
	return segment.Read(offset)
}

// newSegment adds a new segment to `log.segments`, and sets it as the activeSegment.
func (l *Log) newSegment(baseOffset uint64) error {
	s, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}

func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	for _, s := range l.segments {
		if err := s.Close(); err != nil {
			return err
		}
	}
	return nil
}
