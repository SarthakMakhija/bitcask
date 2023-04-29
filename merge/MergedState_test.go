package merge

import (
	"bitcask/kv/log"
	"testing"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestMergeDistinctKeyValuePairs(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:     "topic",
		Value:   []byte("microservices"),
		Deleted: false,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:     "disk",
		Value:   []byte("ssd"),
		Deleted: false,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	if string(mergedState.valueByKey["topic"].Value) != "microservices" {
		t.Fatalf("Expected value to be %v for the key %v, received %v", "microservices", "topic", string(mergedState.valueByKey["topic"].Value))
	}
	if string(mergedState.valueByKey["disk"].Value) != "ssd" {
		t.Fatalf("Expected value to be %v for the key %v, received %v", "ssd", "disk", string(mergedState.valueByKey["disk"].Value))
	}
}

func TestMergeWithDeletionWithHigherTimestamp(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 0,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   true,
		Timestamp: 1,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	value, ok := mergedState.valueByKey["topic"]
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, received %v", "topic", string(value.Value))
	}
}

func TestMergeWithDeletionInTheFirstSet(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   true,
		Timestamp: 0,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 1,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	if string(mergedState.valueByKey["topic"].Value) != "microservices" {
		t.Fatalf("Expected value to be %v for the key %v, received %v", "microservices", "topic", string(mergedState.valueByKey["topic"].Value))
	}
}

func TestMergeWithDeletionInTheFirstSetHavingHighTimestamp(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   true,
		Timestamp: 1,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 0,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	value, ok := mergedState.valueByKey["topic"]
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, received %v", "topic", string(value.Value))
	}
}

func TestMergeWithDeletionWithoutSameEntry(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   true,
		Timestamp: 0,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "disk",
		Value:     []byte("ssd"),
		Deleted:   false,
		Timestamp: 1,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	value, ok := mergedState.valueByKey["topic"]
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, received %v", "topic", string(value.Value))
	}
	if string(mergedState.valueByKey["disk"].Value) != "ssd" {
		t.Fatalf("Expected value to be %v for the key %v, received %v", "ssd", "disk", string(mergedState.valueByKey["disk"].Value))
	}
}

func TestMergeWithUpdate(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 0,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("bitcask"),
		Deleted:   false,
		Timestamp: 1,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	if string(mergedState.valueByKey["topic"].Value) != "bitcask" {
		t.Fatalf("Expected value to be %v for the key %v, received %v", "bitcask", "topic", string(mergedState.valueByKey["topic"].Value))
	}
}

func TestMergeWithUpdateInTheFirstSetHavingHighTimestamp(t *testing.T) {
	mergedState := NewMergedState[serializableKey]()
	entry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("microservices"),
		Deleted:   false,
		Timestamp: 1,
	}
	otherEntry := &log.MappedStoredEntry[serializableKey]{
		Key:       "topic",
		Value:     []byte("bitcask"),
		Deleted:   false,
		Timestamp: 0,
	}
	mergedState.merge([]*log.MappedStoredEntry[serializableKey]{entry}, []*log.MappedStoredEntry[serializableKey]{otherEntry})

	if string(mergedState.valueByKey["topic"].Value) != "microservices" {
		t.Fatalf("Expected value to be %v for the key %v, received %v", "microservices", "topic", string(mergedState.valueByKey["topic"].Value))
	}
}
