package server

import (
	"fmt"
	"sync"
)

var ErrOffsetNotFounc = fmt.Errorf("offset not found")

type Log struct {
	mu      sync.Mutex
	records []Record
}

func NewLog() *Log {
	return &Log{}
}

func (c *Log) Append(r Record) (uint64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	r.Offset = uint64(len(c.records))
	c.records = append(c.records, r)
	return r.Offset, nil
}

func (c *Log) Read(offset uint64) (Record, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if offset >= uint64(len(c.records)) {
		return Record{}, ErrOffsetNotFounc
	}
	return c.records[offset], nil
}

type Record struct {
	Value  []byte `json:"value"`
	Offset uint64 `json:"offset"`
}
