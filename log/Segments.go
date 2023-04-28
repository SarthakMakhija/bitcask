package log

import (
	"bitcask/clock"
	"bitcask/config"
	"bitcask/log/id"
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

func NewSegments[Key config.BitCaskKey](directory string, maxSegmentSizeBytes uint64, clock clock.Clock) (*Segments[Key], error) {
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

func (segments *Segments[Key]) ReadFull(fileId uint64, keyMapper func([]byte) Key) ([]*MappedStoredEntry[Key], error) {
	if fileId == segments.activeSegment.fileId {
		return nil, errors.New(fmt.Sprintf("Can not read active segment with file id %v fully", fileId))
	}
	segment, ok := segments.inactiveSegments[fileId]
	if ok {
		return segment.readFull(keyMapper)
	}
	return nil, errors.New(fmt.Sprintf("Invalid file id %v", fileId))
}

func (segments *Segments[Key]) ReadPairOfInactiveSegments(keyMapper func([]byte) Key) ([][]*MappedStoredEntry[Key], error) {
	if len(segments.inactiveSegments) < 2 {
		return nil, errors.New(fmt.Sprintf("Size of inactive segments is less than 2, actual size is %v", len(segments.inactiveSegments)))
	}

	index := 0
	contents := make([][]*MappedStoredEntry[Key], 2)
	for _, segment := range segments.inactiveSegments {
		if index >= 2 {
			break
		}
		entries, err := segment.readFull(keyMapper)
		if err != nil {
			return nil, err
		}
		contents[index] = entries
		index = index + 1
	}
	return contents, nil
}

func (segments *Segments[Key]) WriteBackInactive(changes map[Key]*MappedStoredEntry[Key]) error {
	segment, err := NewSegment[Key](segments.fileIdGenerator.Next(), segments.directory)
	if err != nil {
		return err
	}
	segments.inactiveSegments[segment.fileId] = segment
	for key, value := range changes {
		_, err := segment.append(NewEntryPreservingTimestamp(key, value.Value, value.Timestamp, segments.clock))
		if err != nil {
			return err
		}
		newSegment, err := segments.maybeRolloverSegment(segment)
		if err != nil {
			return err
		}
		if newSegment != nil {
			segments.inactiveSegments[newSegment.fileId] = newSegment
			segment = newSegment
		}
	}
	return nil
}

func (segments *Segments[Key]) RemoveActive() {
	segments.activeSegment.remove()
}

func (segments *Segments[Key]) RemoveAllInactive() {
	for _, segment := range segments.inactiveSegments {
		segment.remove()
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
