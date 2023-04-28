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

func TestAttemptsToReadAPairOfInactiveSegmentsWhenInActiveSegmentsAreLessThan2(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))

	_, err := segments.ReadPairOfInactiveSegments(func(key []byte) serializableKey {
		return serializableKey(key)
	})
	if err == nil {
		t.Fatalf("Expected an error while reading a pair of inactive segments when the count of inactive segments was less than 2")
	}
}

func TestReadsAPairOfInactiveSegmentsFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("engine", []byte("bitcask"))

	pair, _ := segments.ReadPairOfInactiveSegments(func(key []byte) serializableKey {
		return serializableKey(key)
	})

	entries := pair[0]
	if entries[0].Key != "topic" && entries[0].Key != "diskType" {
		t.Fatalf("Expected key to be either of %v | %v, received %v", "topic", "diskType", entries[0].Key)
	}

	otherEntries := pair[1]
	if otherEntries[0].Key != "topic" && otherEntries[0].Key != "diskType" {
		t.Fatalf("Expected other key to be either of %v | %v, received %v", "topic", "diskType", entries[0].Key)
	}
}
