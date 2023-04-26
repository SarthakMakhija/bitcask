package log

import "testing"

func TestSegmentsWithAnEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100)
	fileId, entryLength, _, _ := segments.Append("topic", []byte("microservices"))

	storedEntry, _ := segments.Read(fileId, 0, uint64(entryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}
