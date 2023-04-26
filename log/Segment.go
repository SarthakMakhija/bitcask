package log

import (
	"fmt"
	"os"
	"path"
)

type StoredEntry struct {
	Key   []byte
	Value []byte
}

type AppendEntryResponse struct {
	FileId      uint64
	Offset      int64
	EntryLength int
}

type Segment[Key Serializable] struct {
	fileId   uint64
	filePath string
	store    *Store
}

const segmentFilePrefix = "bitcask"

func NewSegment[Key Serializable](fileId uint64, directory string) (*Segment[Key], error) {
	filePath, err := createSegment(fileId, directory)
	if err != nil {
		return nil, err
	}
	store, err := NewStore(filePath)
	if err != nil {
		return nil, err
	}
	return &Segment[Key]{
		fileId:   fileId,
		filePath: filePath,
		store:    store,
	}, nil
}

func (segment *Segment[Key]) Append(entry *Entry[Key]) (*AppendEntryResponse, error) {
	encoded := entry.encode()
	offset, err := segment.store.append(encoded)
	if err != nil {
		return nil, err
	}
	return &AppendEntryResponse{
		FileId:      segment.fileId,
		Offset:      offset,
		EntryLength: len(encoded),
	}, nil
}

func (segment *Segment[Key]) Read(offset int64, size uint64) (*StoredEntry, error) {
	bytes, err := segment.store.read(offset, size)
	if err != nil {
		return nil, err
	}
	key, value := decode(bytes)
	return &StoredEntry{Key: key, Value: value}, nil
}

func (segment *Segment[Key]) sizeInBytes() int64 {
	return segment.store.sizeInBytes()
}

func (segment *Segment[Key]) sync() {
	segment.store.sync()
}

func (segment *Segment[Key]) remove() {
	segment.store.remove()
}

func createSegment(fileId uint64, directory string) (string, error) {
	filePath := segmentName(fileId, directory)
	_, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	return filePath, nil
}

func segmentName(fileId uint64, directory string) string {
	return path.Join(directory, fmt.Sprintf("%v_%v", fileId, segmentFilePrefix))
}
