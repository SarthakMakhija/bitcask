package log

import (
	"bitcask/config"
	"fmt"
	"os"
	"path"
)

type StoredEntry struct {
	Key       []byte
	Value     []byte
	Deleted   bool
	Timestamp uint32
}

type MappedStoredEntry[K config.BitCaskKey] struct {
	Key       K
	Value     []byte
	Deleted   bool
	Timestamp uint32
}

type AppendEntryResponse struct {
	FileId      uint64
	Offset      int64
	EntryLength int
}

type Segment[Key config.BitCaskKey] struct {
	fileId   uint64
	filePath string
	store    *Store
}

const segmentFilePrefix = "bitcask"

func NewSegment[Key config.BitCaskKey](fileId uint64, directory string) (*Segment[Key], error) {
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

func (segment *Segment[Key]) append(entry *Entry[Key]) (*AppendEntryResponse, error) {
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

func (segment *Segment[Key]) read(offset int64, size uint64) (*StoredEntry, error) {
	bytes, err := segment.store.read(offset, size)
	if err != nil {
		return nil, err
	}
	storedEntry := decode(bytes)
	return storedEntry, nil
}

func (segment *Segment[Key]) readFull(keyMapper func([]byte) Key) ([]*MappedStoredEntry[Key], error) {
	bytes, err := segment.store.readFull()
	if err != nil {
		return nil, err
	}
	storedEntries := decodeMulti(bytes, keyMapper)
	return storedEntries, nil
}

func (segment *Segment[Key]) sizeInBytes() int64 {
	return segment.store.sizeInBytes()
}

func (segment *Segment[Key]) sync() {
	segment.store.sync()
}

func (segment *Segment[Key]) stopWrites() {
	segment.store.stopWrites()
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
	return path.Join(directory, fmt.Sprintf("%v_%v.data", fileId, segmentFilePrefix))
}
