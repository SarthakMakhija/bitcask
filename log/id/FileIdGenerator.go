package id

import (
	"bitcask/clock"
)

type FileIdGenerator struct {
	clock clock.Clock
}

func NewFileIdGenerator(clock clock.Clock) *FileIdGenerator {
	return &FileIdGenerator{clock: clock}
}

func (generator *FileIdGenerator) Next() uint64 {
	return uint64(generator.clock.Now())
}
