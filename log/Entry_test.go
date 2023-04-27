package log

import (
	"bitcask/clock"
	"testing"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestEncodesAKeyValuePair(t *testing.T) {
	entry := NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock())
	encoded := entry.encode()

	storedEntry := decode(encoded)
	if storedEntry.Deleted {
		t.Fatalf("Expected key to not be deleted, but was deleted")
	}
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected decoded key to be %v, received %v", "topic", string(storedEntry.Key))
	}
	if string(storedEntry.Value) != "microservices" {
		t.Fatalf("Expected decoded value to be %v, received %v", "microservices", string(storedEntry.Value))
	}
}

func TestEncodesADeletedKeyValuePair(t *testing.T) {
	entry := NewDeletedEntry[serializableKey]("topic", clock.NewSystemClock())
	encoded := entry.encode()

	storedEntry := decode(encoded)
	if !storedEntry.Deleted {
		t.Fatalf("Expected key to be deleted, but was not")
	}
	if string(storedEntry.Key) != "topic" {
		t.Fatalf("Expected decoded key to be %v, received %v", "topic", string(storedEntry.Key))
	}
}
