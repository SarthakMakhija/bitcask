package id

import "sync/atomic"

type FileIdGenerator struct {
	id uint64
}

func NewFileIdGenerator() *FileIdGenerator {
	return &FileIdGenerator{id: 0}
}

func (generator *FileIdGenerator) Next() uint64 {
	return atomic.AddUint64(&generator.id, 1)
}
