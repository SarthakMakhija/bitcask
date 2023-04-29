package log

import (
	"bitcask/clock"
	"bitcask/config"
	"bitcask/kv/log/id"
	"errors"
	"fmt"
)

type Segments[Key config.BitCaskKey] struct {
	activeSegment       *Segment[Key]
	inactiveSegments    map[uint64]*Segment[Key]
	fileIdGenerator     *id.FileIdGenerator
	clock               clock.Clock
	maxSegmentSizeBytes uint64
	directory           string
}

type WriteBackResponse[K config.BitCaskKey] struct {
	Key                 K
	AppendEntryResponse *AppendEntryResponse
}

func NewSegments[Key config.BitCaskKey](directory string, maxSegmentSizeBytes uint64, clock clock.Clock) (*Segments[Key], error) {
	fileIdGenerator := id.NewFileIdGenerator(clock)
	fileId := fileIdGenerator.Next()
	activeSegment, err := NewSegment[Key](fileId, directory)
	if err != nil {
		return nil, err
	}

	return &Segments[Key]{
		activeSegment:       activeSegment,
		inactiveSegments:    make(map[uint64]*Segment[Key]),
		fileIdGenerator:     fileIdGenerator,
		clock:               clock,
		maxSegmentSizeBytes: maxSegmentSizeBytes,
		directory:           directory,
	}, nil
}

func (segments *Segments[Key]) Append(key Key, value []byte) (*AppendEntryResponse, error) {
	if err := segments.maybeRolloverActiveSegment(); err != nil {
		return nil, err
	}
	return segments.activeSegment.append(NewEntry[Key](key, value, segments.clock))
}

func (segments *Segments[Key]) AppendDeleted(key Key) (*AppendEntryResponse, error) {
	if err := segments.maybeRolloverActiveSegment(); err != nil {
		return nil, err
	}
	return segments.activeSegment.append(NewDeletedEntry[Key](key, segments.clock))
}

func (segments *Segments[Key]) Read(fileId uint64, offset int64, size uint64) (*StoredEntry, error) {
	if fileId == segments.activeSegment.fileId {
		return segments.activeSegment.read(offset, size)
	}
	segment, ok := segments.inactiveSegments[fileId]
	if ok {
		return segment.read(offset, size)
	}
	return nil, errors.New(fmt.Sprintf("Invalid file id %v", fileId))
}

func (segments *Segments[Key]) ReadInactiveSegments(totalSegments int, keyMapper func([]byte) Key) ([]uint64, [][]*MappedStoredEntry[Key], error) {
	index := 0
	contents, fileIds := make([][]*MappedStoredEntry[Key], totalSegments), make([]uint64, totalSegments)
	for _, segment := range segments.inactiveSegments {
		if index >= totalSegments {
			break
		}
		entries, err := segment.readFull(keyMapper)
		if err != nil {
			return nil, nil, err
		}
		contents[index] = entries
		fileIds[index] = segment.fileId
		index = index + 1
	}
	return fileIds, contents, nil
}

func (segments *Segments[Key]) ReadAllInactiveSegments(keyMapper func([]byte) Key) ([]uint64, [][]*MappedStoredEntry[Key], error) {
	return segments.ReadInactiveSegments(len(segments.inactiveSegments), keyMapper)
}

func (segments *Segments[Key]) WriteBack(changes map[Key]*MappedStoredEntry[Key]) ([]*WriteBackResponse[Key], error) {
	segment, err := NewSegment[Key](segments.fileIdGenerator.Next(), segments.directory)
	if err != nil {
		return nil, err
	}
	segments.inactiveSegments[segment.fileId] = segment

	index, writeBackResponses := 0, make([]*WriteBackResponse[Key], len(changes))
	for key, value := range changes {
		appendEntryResponse, err := segment.append(NewEntryPreservingTimestamp(key, value.Value, value.Timestamp, segments.clock))
		if err != nil {
			return nil, err
		}
		writeBackResponses[index] = &WriteBackResponse[Key]{Key: key, AppendEntryResponse: appendEntryResponse}
		index = index + 1

		newSegment, err := segments.maybeRolloverSegment(segment)
		if err != nil {
			return nil, err
		}
		if newSegment != nil {
			segments.inactiveSegments[newSegment.fileId] = newSegment
			segment = newSegment
		}
	}
	return writeBackResponses, nil
}

func (segments *Segments[Key]) RemoveActive() {
	segments.activeSegment.remove()
}

func (segments *Segments[Key]) RemoveAllInactive() {
	for _, segment := range segments.inactiveSegments {
		segment.remove()
	}
}

func (segments *Segments[Key]) Remove(fileIds []uint64) {
	for _, fileId := range fileIds {
		segment, ok := segments.inactiveSegments[fileId]
		if ok {
			segment.remove()
			delete(segments.inactiveSegments, fileId)
		}
	}
}

func (segments *Segments[Key]) maybeRolloverActiveSegment() error {
	newSegment, err := segments.maybeRolloverSegment(segments.activeSegment)
	if err != nil {
		return err
	}
	if newSegment != nil {
		segments.inactiveSegments[segments.activeSegment.fileId] = segments.activeSegment
		segments.activeSegment = newSegment
	}
	return nil
}

func (segments *Segments[Key]) maybeRolloverSegment(segment *Segment[Key]) (*Segment[Key], error) {
	if segment.sizeInBytes() >= int64(segments.maxSegmentSizeBytes) {
		segment.stopWrites()
		newSegment, err := NewSegment[Key](segments.fileIdGenerator.Next(), segments.directory)
		if err != nil {
			return nil, err
		}
		return newSegment, nil
	}
	return nil, nil
}
