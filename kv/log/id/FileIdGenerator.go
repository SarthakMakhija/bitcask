package id

import (
	"bitcask/clock"
)

// TimestampBasedFileIdGenerator generates a new file based on the clock provided.
type TimestampBasedFileIdGenerator struct {
	clock clock.Clock
}

// NewTimestampBasedFileIdGenerator creates a new instance of TimestampBasedFileIdGenerator
func NewTimestampBasedFileIdGenerator(clock clock.Clock) *TimestampBasedFileIdGenerator {
	return &TimestampBasedFileIdGenerator{clock: clock}
}

// Next generates the new file id based on the current time of the clock
func (generator *TimestampBasedFileIdGenerator) Next() uint64 {
	return uint64(generator.clock.Now())
}
