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
	Key         K
	Value       []byte
	Deleted     bool
	Timestamp   uint32
	KeyOffset   uint32
	EntryLength uint32
}

type AppendEntryResponse struct {
	FileId      uint64
	Offset      int64
	EntryLength uint32
}

type Segment[Key config.BitCaskKey] struct {
	fileId   uint64
	filePath string
	store    *Store
}

const segmentFilePrefix = "bitcask"
const segmentFileSuffix = "data"

// NewSegment represents an append-only log
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

// ReloadInactiveSegment reloads the inactive segment during start-up. As a part of ReloadInactiveSegment, we just create the in-memory representation of inactive segment and its store
func ReloadInactiveSegment[Key config.BitCaskKey](fileId uint64, directory string) (*Segment[Key], error) {
	filePath := segmentName(fileId, directory)
	store, err := ReloadStore(filePath)
	if err != nil {
		return nil, err
	}
	return &Segment[Key]{
		fileId:   fileId,
		filePath: filePath,
		store:    store,
	}, nil
}

// append performs an append operation in the segment file. Append operation is a 2-step process:
// 1. Encode the incoming entry, more on this in Entry.go
// 2. Write the encoded entry ([]byte) to the segment file using the Store abstraction
func (segment *Segment[Key]) append(entry *Entry[Key]) (*AppendEntryResponse, error) {
	encoded := entry.encode()
	offset, err := segment.store.append(encoded)
	if err != nil {
		return nil, err
	}
	return &AppendEntryResponse{
		FileId:      segment.fileId,
		Offset:      offset,
		EntryLength: uint32(len(encoded)),
	}, nil
}

// read performs a read operation from the offset in the segment file. This method is invoked in the Get operation
func (segment *Segment[Key]) read(offset int64, size uint32) (*StoredEntry, error) {
	bytes, err := segment.store.read(offset, size)
	if err != nil {
		return nil, err
	}
	storedEntry := decode(bytes)
	return storedEntry, nil
}

// ReadFull performs a full read of the segment file. This method is called by the reload operation that happens during DB start-up
func (segment *Segment[Key]) ReadFull(keyMapper func([]byte) Key) ([]*MappedStoredEntry[Key], error) {
	bytes, err := segment.store.readFull()
	if err != nil {
		return nil, err
	}
	storedEntries := decodeMulti(bytes, keyMapper)
	return storedEntries, nil
}

// sizeInBytes returns the segment file size in bytes
func (segment *Segment[Key]) sizeInBytes() int64 {
	return segment.store.sizeInBytes()
}

// sync Performs a file sync, ensures all the disk blocks (or pages) at the Kernel page cache are flushed to the disk
func (segment *Segment[Key]) sync() {
	segment.store.sync()
}

// stopWrites Closes the write file pointer. This operation is called when the active segment has reached its size threshold.
func (segment *Segment[Key]) stopWrites() {
	segment.store.stopWrites()
}

// remove Removes the file
func (segment *Segment[Key]) remove() {
	segment.store.remove()
}

// createSegment creates a new segment file. Each segment file has a fixed name format. It is fileId_bitcask.data. FileId is the timestamp based on the clock provided.
// FileId is generated by TimestampBasedFileIdGenerator
func createSegment(fileId uint64, directory string) (string, error) {
	filePath := segmentName(fileId, directory)
	_, err := os.Create(filePath)
	if err != nil {
		return "", err
	}
	return filePath, nil
}

func segmentName(fileId uint64, directory string) string {
	return path.Join(directory, fmt.Sprintf("%v_%v.%v", fileId, segmentFilePrefix, segmentFileSuffix))
}
