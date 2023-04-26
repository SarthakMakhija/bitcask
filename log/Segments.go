package log

import (
	"bitcask/id"
	"errors"
	"fmt"
)

type Segments[Key Serializable] struct {
	activeSegment    *Segment[Key]
	inactiveSegments map[uint64]*Segment[Key]
	fileIdGenerator  *id.FileIdGenerator
	segmentSizeBytes uint64
	directory        string
}

func NewSegments[Key Serializable](directory string, segmentSizeBytes uint64) (*Segments[Key], error) {
	fileIdGenerator := id.NewFileIdGenerator()
	fileId := fileIdGenerator.Next()
	segment, err := NewSegment[Key](fileId, directory)
	if err != nil {
		return nil, err
	}

	return &Segments[Key]{
		activeSegment:    segment,
		inactiveSegments: make(map[uint64]*Segment[Key]), //TODO: capacity
		fileIdGenerator:  fileIdGenerator,
		segmentSizeBytes: segmentSizeBytes,
		directory:        directory,
	}, nil
}

func (segments *Segments[Key]) Append(key Key, value []byte) (*AppendEntryResponse, error) {
	maybeRolloverSegment := func() error {
		if segments.activeSegment.sizeInBytes() >= int64(segments.segmentSizeBytes) {
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
	if err := maybeRolloverSegment(); err != nil {
		return nil, err
	}
	return segments.activeSegment.Append(NewEntry[Key](key, value))
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

func (segments *Segments[Key]) removeActive() {
	segments.activeSegment.remove()
}

func (segments *Segments[Key]) removeAllInactive() {
	for _, segment := range segments.inactiveSegments {
		segment.remove()
	}
}
