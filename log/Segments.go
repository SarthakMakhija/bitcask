package log

import (
	"bitcask/clock"
	"bitcask/key"
	"bitcask/log/id"
	"errors"
	"fmt"
)

type Segments[Key key.Serializable] struct {
	activeSegment       *Segment[Key]
	inactiveSegments    map[uint64]*Segment[Key]
	fileIdGenerator     *id.FileIdGenerator
	clock               clock.Clock
	maxSegmentSizeBytes uint64
	directory           string
}

func NewSegments[Key key.Serializable](directory string, maxSegmentSizeBytes uint64, clock clock.Clock) (*Segments[Key], error) {
	fileIdGenerator := id.NewFileIdGenerator()
	fileId := fileIdGenerator.Next()
	segment, err := NewSegment[Key](fileId, directory)
	if err != nil {
		return nil, err
	}

	return &Segments[Key]{
		activeSegment:       segment,
		inactiveSegments:    make(map[uint64]*Segment[Key]), //TODO: capacity
		fileIdGenerator:     fileIdGenerator,
		clock:               clock,
		maxSegmentSizeBytes: maxSegmentSizeBytes,
		directory:           directory,
	}, nil
}

func (segments *Segments[Key]) Append(key Key, value []byte) (*AppendEntryResponse, error) {
	if err := segments.maybeRolloverSegment(); err != nil {
		return nil, err
	}
	return segments.activeSegment.Append(NewEntry[Key](key, value, segments.clock))
}

func (segments *Segments[Key]) AppendDeleted(key Key) (*AppendEntryResponse, error) {
	if err := segments.maybeRolloverSegment(); err != nil {
		return nil, err
	}
	return segments.activeSegment.Append(NewDeletedEntry[Key](key, segments.clock))
}

func (segments *Segments[Key]) Read(fileId uint64, offset int64, size uint64) (*StoredEntry, error) {
	if fileId == segments.activeSegment.fileId {
		return segments.activeSegment.Read(offset, size)
	}
	segment, ok := segments.inactiveSegments[fileId]
	if ok {
		return segment.Read(offset, size)
	}
	return nil, errors.New(fmt.Sprintf("Invalid file id %v", fileId))
}

func (segments *Segments[Key]) RemoveActive() {
	segments.activeSegment.remove()
}

func (segments *Segments[Key]) RemoveAllInactive() {
	for _, segment := range segments.inactiveSegments {
		segment.remove()
	}
}

func (segments *Segments[Key]) maybeRolloverSegment() error {
	if segments.activeSegment.sizeInBytes() >= int64(segments.maxSegmentSizeBytes) {
		segment, err := NewSegment[Key](segments.fileIdGenerator.Next(), segments.directory)
		if err != nil {
			return err
		}
		segments.inactiveSegments[segments.activeSegment.fileId] = segments.activeSegment
		segments.activeSegment = segment
		return nil
	}
	return nil
}
