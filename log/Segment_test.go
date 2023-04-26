package log

import (
	"testing"
)

func TestNewSegmentWithAnEntry(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	entryLength, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))

	storedEntry, _ := segment.Read(0, uint64(entryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestNewSegmentWithAnEntryAndPerformSync(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	entryLength, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	segment.sync()

	storedEntry, _ := segment.Read(0, uint64(entryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestNewSegmentWith2Entries(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	entryLengthTopic, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	entryLengthDisk, _ := segment.Append(NewEntry[serializableKey]("disk", []byte("ssd")))

	storedEntry, _ := segment.Read(int64(entryLengthTopic), uint64(entryLengthDisk))
	if string(storedEntry.Key) != "disk" {
		t.Fatalf("Expected key to be %v, received %v", "disk", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "ssd" {
		t.Fatalf("Expected value to be %v, received %v", "ssd", string(storedEntry.Value))
	}
}
