package log

import (
	"bitcask/clock"
	"testing"
)

func TestNewSegmentWithAnEntry(t *testing.T) {
	segment, _ := NewSegment[serializableKey](1, ".")
	defer func() {
		segment.remove()
	}()

	appendEntryResponse, _ := segment.append(NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock()))

	storedEntry, _ := segment.read(appendEntryResponse.Offset, appendEntryResponse.EntryLength)
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

	appendEntryResponse, _ := segment.append(NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock()))
	segment.sync()

	storedEntry, _ := segment.read(appendEntryResponse.Offset, appendEntryResponse.EntryLength)
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

	_, _ = segment.append(NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock()))
	appendEntryResponseDisk, _ := segment.append(NewEntry[serializableKey]("disk", []byte("ssd"), clock.NewSystemClock()))

	storedEntry, _ := segment.read(appendEntryResponseDisk.Offset, appendEntryResponseDisk.EntryLength)
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

	appendEntryResponseTopic, _ := segment.append(NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock()))
	appendEntryResponseDisk, _ := segment.append(NewEntry[serializableKey]("disk", []byte("ssd"), clock.NewSystemClock()))

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

	appendEntryResponse, _ := segment.append(NewDeletedEntry[serializableKey]("topic", clock.NewSystemClock()))

	storedEntry, _ := segment.read(appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if !storedEntry.Deleted {
		t.Fatalf("Expected key to be deleted, but was not")
	}
}

func TestNewSegmentByReadingFull(t *testing.T) {
	segment, _ := NewSegment[serializableKey](4, ".")
	defer func() {
		segment.remove()
	}()

	_, _ = segment.append(NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock()))
	_, _ = segment.append(NewEntry[serializableKey]("disk", []byte("ssd"), clock.NewSystemClock()))

	entries, _ := segment.ReadFull(func(key []byte) serializableKey {
		return serializableKey(key)
	})

	if entries[0].Key != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", entries[0].Key)
	}
	if string(entries[0].Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(entries[0].Value))
	}

	if entries[1].Key != "disk" {
		t.Fatalf("Expected key to be %v, received %v", "disk", entries[1].Key)
	}
	if string(entries[1].Value) != "ssd" {
		t.Fatalf("Expected value to be %v, received %v", "ssd", string(entries[1].Value))
	}
}

func TestNewSegmentAfterStoppingWrites(t *testing.T) {
	segment, _ := NewSegment[serializableKey](2, ".")
	defer func() {
		segment.remove()
	}()

	appendEntryResponse, _ := segment.append(NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock()))
	segment.stopWrites()

	storedEntry, _ := segment.read(appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}

	_, err := segment.append(NewEntry[serializableKey]("after-stopping", []byte("true"), clock.NewSystemClock()))
	if err == nil {
		t.Fatalf("Expected error while writing to the segment after it was write closed but no error was received")
	}
}
