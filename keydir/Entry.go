package keydir

import "bitcask/log"

type Entry struct {
	FileId      uint64
	Offset      int64
	EntryLength int
}

func NewEntryFrom(response *log.AppendEntryResponse) *Entry {
	return NewEntry(response.FileId, response.Offset, response.EntryLength)
}

func NewEntry(fileId uint64, offset int64, entryLength int) *Entry {
	return &Entry{
		FileId:      fileId,
		Offset:      offset,
		EntryLength: entryLength,
	}
}
