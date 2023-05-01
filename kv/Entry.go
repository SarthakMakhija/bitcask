package kv

import (
	"bitcask/kv/log"
)

// Entry (pointer to the Entry) is used as a value in the KeyDirectory
// It identifies the file containing the key, the offset of the key-value in the file and the entry length.
// Refer to Entry.go inside log/ package to understand encoding and decoding.
type Entry struct {
	FileId      uint64
	Offset      int64
	EntryLength uint32
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
