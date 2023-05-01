package log

import (
	"bitcask/clock"
	"reflect"
	"sort"
	"testing"
)

func TestReadActiveSegmentWithAnEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendEntryResponse, _ := segments.Append("topic", []byte("microservices"))

	storedEntry, _ := segments.Read(appendEntryResponse.FileId, appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestReadAnInactiveSegmentInvolvingRollover(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 32, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendEntryResponseTopic, _ := segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("databaseType", []byte("distributed"))

	storedEntry, _ := segments.Read(appendEntryResponseTopic.FileId, appendEntryResponseTopic.Offset, appendEntryResponseTopic.EntryLength)
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
		segments.RemoveAllInactive()
	}()

	appendEntryResponse, _ := segments.Append("topic", []byte("microservices"))

	_, err := segments.Read(10, appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	if err == nil {
		t.Fatalf("Expected an error while reading a segment with an invalid file id but received none")
	}
}

func TestReadASegmentWithADeletedEntry(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 100, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendEntryResponse, _ := segments.AppendDeleted("topic")

	storedEntry, _ := segments.Read(appendEntryResponse.FileId, appendEntryResponse.Offset, appendEntryResponse.EntryLength)
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if !storedEntry.Deleted {
		t.Fatalf("Expected key to be deleted, but was not")
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

	_, pair, _ := segments.ReadInactiveSegments(2, func(key []byte) serializableKey {
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

func TestReadsAllInactiveSegmentsFull(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	_, _ = segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("engine", []byte("bitcask"))

	_, pair, _ := segments.ReadAllInactiveSegments(func(key []byte) serializableKey {
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

func TestWriteBackInvolvingRollover(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	changes := make(map[serializableKey]*MappedStoredEntry[serializableKey])
	changes["disk"] = &MappedStoredEntry[serializableKey]{Value: []byte("solid state drive")}
	changes["engine"] = &MappedStoredEntry[serializableKey]{Value: []byte("bitcask")}
	changes["topic"] = &MappedStoredEntry[serializableKey]{Value: []byte("Microservices")}

	_, _ = segments.WriteBack(changes)

	allKeys := allInactiveSegmentsKeys(segments)
	expectedKeys := []serializableKey{"disk", "engine", "topic"}

	if !reflect.DeepEqual(expectedKeys, allKeys) {
		t.Fatalf("Expected all keys from inactive segments to be %v, received %v", expectedKeys, allKeys)
	}
}

func TestWriteBackNotInvolvingRollover(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 256, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	changes := make(map[serializableKey]*MappedStoredEntry[serializableKey])
	changes["disk"] = &MappedStoredEntry[serializableKey]{Value: []byte("solid state drive")}
	changes["engine"] = &MappedStoredEntry[serializableKey]{Value: []byte("bitcask")}
	changes["topic"] = &MappedStoredEntry[serializableKey]{Value: []byte("Microservices")}

	_, _ = segments.WriteBack(changes)

	allKeys := allInactiveSegmentsKeys(segments)
	expectedKeys := []serializableKey{"disk", "engine", "topic"}

	if !reflect.DeepEqual(expectedKeys, allKeys) {
		t.Fatalf("Expected all keys from inactive segments to be %v, received %v", expectedKeys, allKeys)
	}
}

func TestRemoveInactiveSegmentById(t *testing.T) {
	segments, _ := NewSegments[serializableKey](".", 8, clock.NewSystemClock())
	defer func() {
		segments.RemoveActive()
		segments.RemoveAllInactive()
	}()

	appendEntryResponseTopic, _ := segments.Append("topic", []byte("microservices"))
	_, _ = segments.Append("diskType", []byte("solid state drive"))
	_, _ = segments.Append("databaseType", []byte("distributed"))

	_, ok := segments.inactiveSegments[appendEntryResponseTopic.FileId]
	if !ok {
		t.Fatalf("Expected %v to be an inactive segment but was not", appendEntryResponseTopic.FileId)
	}
	segments.Remove([]uint64{appendEntryResponseTopic.FileId})

	_, ok = segments.inactiveSegments[appendEntryResponseTopic.FileId]
	if ok {
		t.Fatalf("Expected %v to not be an inactive segment but was", appendEntryResponseTopic.FileId)
	}
}

func allInactiveSegmentsKeys(segments *Segments[serializableKey]) []serializableKey {
	var allKeys []serializableKey
	for _, segment := range segments.inactiveSegments {
		contents, _ := segment.ReadFull(func(key []byte) serializableKey {
			return serializableKey(key)
		})
		for _, content := range contents {
			allKeys = append(allKeys, content.Key)
		}
	}
	sort.SliceStable(allKeys, func(i, j int) bool {
		return allKeys[i] < allKeys[j]
	})
	return allKeys
}
