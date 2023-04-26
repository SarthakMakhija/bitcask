package log

import (
	"testing"
)

func TestNewSegmentWithAnEntry(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	defer func() {
		segment.remove()
	}()

	entryLength, _, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))

	storedEntry, _ := segment.Read(0, uint64(entryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestNewSegmentWithAnEntryAndPerformSync(t *testing.T) {
	segment, _ := NewSegment[serializableKey](2, ".")
	defer func() {
		segment.remove()
	}()

	entryLength, _, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
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
	segment, _ := NewSegment[serializableKey](3, ".")
	defer func() {
		segment.remove()
	}()

	entryLengthTopic, _, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	entryLengthDisk, _, _ := segment.Append(NewEntry[serializableKey]("disk", []byte("ssd")))

	storedEntry, _ := segment.Read(int64(entryLengthTopic), uint64(entryLengthDisk))
	if string(storedEntry.Key) != "disk" {
		t.Fatalf("Expected key to be %v, received %v", "disk", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "ssd" {
		t.Fatalf("Expected value to be %v, received %v", "ssd", string(storedEntry.Value))
	}
}

func TestNewSegmentWith2EntriesAndValidateOffset(t *testing.T) {
	segment, _ := NewSegment[serializableKey](4, ".")
	defer func() {
		segment.remove()
	}()

	entryLength, offset, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	_, anotherOffset, _ := segment.Append(NewEntry[serializableKey]("disk", []byte("ssd")))

	if offset != 0 {
		t.Fatalf("Expected initial offset to be %v, received %v", 0, offset)
	}
	if anotherOffset != int64(entryLength) {
		t.Fatalf("Expected another offset to be %v, received %v", entryLength, offset)
	}
}
