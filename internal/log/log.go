package log

import (
	"fmt"
	api "github.com/jxofficial/log/api/v1"
	"io"
	"io/ioutil"
	"os"
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

// setup setups the log using the store and index files in `log.Dir`
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

// Close closes all the segments.
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

// Remove closes the log and removes all the associated files.
func (l *Log) Remove() error {
	if err := l.Close(); err != nil {
		return err
	}
	return os.RemoveAll(l.Dir)
}

// Reset removes the log and creates a new log.
func (l *Log) Reset() error {
	if err := l.Remove(); err != nil {
		return err
	}
	return l.setup()
}

// helper methods

// LowestOffset returns the offset of the earliest record in the log.
func (l *Log) LowestOffset() (uint64, error) {
	l.mu.RLock()
	l.mu.RUnlock()
	return l.segments[0].baseOffset, nil
}

// HighestOffset returns the largest offset in the log.
func (l *Log) HighestOffset() (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	offset := l.segments[len(l.segments)-1].nextOffset
	if offset == 0 {
		return 0, nil
	}
	return offset - 1, nil
}

// Truncate removes all segments whose highest offset is lower or equal to the `lowest` argument.
func (l *Log) Truncate(lowest uint64) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	var segments []*segment
	for _, s := range l.segments {
		if s.nextOffset-1 <= lowest {
			if err := s.Remove(); err != nil {
				return err
			}
			continue
		}
		segments = append(segments, s)
	}
	l.segments = segments
	return nil
}

func (l *Log) Reader() io.Reader {
	l.mu.RLock()
	defer l.mu.RUnlock()
	readers := make([]io.Reader, len(l.segments))
	for i, s := range l.segments {
		readers[i] = &originalReader{s.store, 0}
	}
	return io.MultiReader(readers...)
}

type originalReader struct {
	*store
	offset int64
}

func (o *originalReader) Read(p []byte) (int, error) {
	n, err := o.ReadAt(p, o.offset)
	o.offset += int64(n)
	return n, err
}

// newSegment appends a new segment to `log.segments`, and sets it as the activeSegment.
func (l *Log) newSegment(baseOffset uint64) error {
	s, err := newSegment(l.Dir, baseOffset, l.Config)
	if err != nil {
		return err
	}
	l.segments = append(l.segments, s)
	l.activeSegment = s
	return nil
}
