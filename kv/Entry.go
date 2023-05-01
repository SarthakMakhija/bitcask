package kv

import (
	"bitcask/kv/log"
)

type Entry struct {
	FileId      uint64
	Offset      int64
	EntryLength uint32
}

type KeyEntryPair[K comparable] struct {
	Key   K
	entry *Entry
}

func NewEntryFrom(response *log.AppendEntryResponse) *Entry {
	return NewEntry(response.FileId, response.Offset, response.EntryLength)
}

func NewEntry(fileId uint64, offset int64, entryLength uint32) *Entry {
	return &Entry{
		FileId:      fileId,
		Offset:      offset,
		EntryLength: entryLength,
	}
}
