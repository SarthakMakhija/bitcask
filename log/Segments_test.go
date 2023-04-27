package log

import (
	"bitcask/clock"
	"testing"
)

func TestReadActiveSegmentWithAnEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
	}()

	appendEntryResponse, _ := segments.Append("topic", []byte("microservices"))

	storedEntry, _ := segments.Read(appendEntryResponse.FileId, appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestReadAnInactiveSegmentWith(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 32, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendEntryResponseTopic, _ := segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("databaseType", []byte("distributed"))

	storedEntry, _ := segments.Read(appendEntryResponseTopic.FileId, appendEntryResponseTopic.Offset, uint64(appendEntryResponseTopic.EntryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestAttemptsToReadInvalidSegment(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
	}()

	appendEntryResponse, _ := segments.Append("topic", []byte("microservices"))

	_, err := segments.Read(10, appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
	if err == nil {
		t.Fatalf("Expected an error while reading a segment with an invalid file id but received none")
	}
}

func TestReadASegmentWithADeletedEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
	}()

	appendEntryResponse, _ := segments.AppendDeleted("topic")

	storedEntry, _ := segments.Read(appendEntryResponse.FileId, appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if !storedEntry.Deleted {
		t.Fatalf("Expected key to be deleted, but was not")
	}
}

func TestAttemptsToReadAnActiveSegmentFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 32, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))
	_, err := segments.ReadFull(segments.activeSegment.fileId)

	if err == nil {
		t.Fatalf("Expected an error while attempting to read the active segment full")
	}
}

func TestReadsAnActiveSegmentFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 16, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))

	entries, _ := segments.ReadFull(1)

	if len(entries) != 1 {
		t.Fatalf("Expected length of entries to be 1, received %v", len(entries))
	}
	if string(entries[0].Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(entries[0].Key))
	}
	if string(entries[0].Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(entries[0].Value))
	}
}

func TestAttemptsToReadInvalidSegmentFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))

	_, err := segments.ReadFull(10)
	if err == nil {
		t.Fatalf("Expected an error while reading a segment with an invalid file id but received none")
	}
}
