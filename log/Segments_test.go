package log

import "testing"

func TestReadActiveSegmentWithAnEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100)
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
	segments, _ := NewSegments[serializableKey](".", 32)
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

func TestReadInvalidSegment(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100)
	defer func() {
		segments.RemoveActive()
	}()

	appendEntryResponse, _ := segments.Append("topic", []byte("microservices"))

	_, err := segments.Read(10, appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
	if err == nil {
		t.Fatalf("Expected an error while reading a segment with an invalid file id but received none")
	}
}
