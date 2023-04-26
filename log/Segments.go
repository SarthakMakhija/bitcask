package log

import (
	"bitcask/id"
)

type Segments[Key Serializable] struct {
	active           *Segment[Key]
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
		active:           segment,
		fileIdGenerator:  fileIdGenerator,
		segmentSizeBytes: segmentSizeBytes,
		directory:        directory,
	}, nil
}

func (segments *Segments[Key]) Append(key Key, value []byte) (*AppendEntryResponse, error) {
	return segments.active.Append(NewEntry[Key](key, value))
}

func (segments *Segments[Key]) Read(fileId uint64, position int64, size uint64) (*StoredEntry, error) {
	//TODO: fetch from the segment matching the file id
	return segments.active.Read(position, size)
}

func (segments *Segments[Key]) removeActive() {
	segments.active.remove()
}
