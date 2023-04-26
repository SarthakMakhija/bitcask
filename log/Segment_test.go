package log

import (
	"testing"
)

func TestNewSegmentWithAnEntry(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	defer func() {
		segment.remove()
	}()

	appendEntryResponse, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))

	storedEntry, _ := segment.Read(appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
	if storedEntry.Deleted {
		t.Fatalf("Expected key to not be deleted, but was deleted")
	}
}

func TestNewSegmentWithAnEntryAndPerformSync(t *testing.T) {
	segment, _ := NewSegment[serializableKey](2, ".")
	defer func() {
		segment.remove()
	}()

	appendEntryResponse, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	segment.sync()

	storedEntry, _ := segment.Read(appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
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

	_, _ = segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	appendEntryResponseDisk, _ := segment.Append(NewEntry[serializableKey]("disk", []byte("ssd")))

	storedEntry, _ := segment.Read(appendEntryResponseDisk.Offset, uint64(appendEntryResponseDisk.EntryLength))
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

	appendEntryResponseTopic, _ := segment.Append(NewEntry[serializableKey]("topic", []byte("microservices")))
	appendEntryResponseDisk, _ := segment.Append(NewEntry[serializableKey]("disk", []byte("ssd")))

	if appendEntryResponseTopic.Offset != 0 {
		t.Fatalf("Expected initial offset to be %v, received %v", 0, appendEntryResponseTopic.Offset)
	}
	if appendEntryResponseDisk.Offset != int64(appendEntryResponseTopic.EntryLength) {
		t.Fatalf("Expected another offset to be %v, received %v", appendEntryResponseTopic.EntryLength, appendEntryResponseDisk.Offset)
	}
}

func TestNewSegmentWithADeletedEntry(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	defer func() {
		segment.remove()
	}()

	appendEntryResponse, _ := segment.Append(NewDeletedEntry[serializableKey]("topic"))

	storedEntry, _ := segment.Read(appendEntryResponse.Offset, uint64(appendEntryResponse.EntryLength))
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if !storedEntry.Deleted {
		t.Fatalf("Expected key to be deleted, but was not")
	}
}
