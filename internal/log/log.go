package log

import (
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
