package id

import (
	"bitcask/clock"
)

type TimestampBasedFileIdGenerator struct {
	clock clock.Clock
}

func NewTimestampBasedFileIdGenerator(clock clock.Clock) *TimestampBasedFileIdGenerator {
	return &TimestampBasedFileIdGenerator{clock: clock}
}

// Next TODO: concurrency
func (generator *TimestampBasedFileIdGenerator) Next() uint64 {
	return uint64(generator.clock.Now())
}
