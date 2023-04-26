package keydir

type Entry struct {
	FileId      uint64
	Offset      int64
	EntryLength int
}

func NewEntry(fileId uint64, offset int64, entryLength int) *Entry {
	return &Entry{
		FileId:      fileId,
		Offset:      offset,
		EntryLength: entryLength,
	}
}
