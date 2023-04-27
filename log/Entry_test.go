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

func TestDecodesMultipleKeyValuePairs(t *testing.T) {
	entry := NewEntry[serializableKey]("topic", []byte("microservices"), clock.NewSystemClock())
	encodedTopic := entry.encode()

	entry = NewEntry[serializableKey]("disk", []byte("ssd"), clock.NewSystemClock())
	encodedDisk := entry.encode()

	entry = NewEntry[serializableKey]("engine", []byte("bitcask"), clock.NewSystemClock())
	encodedEngine := entry.encode()

	multipleEntries := append(append(encodedTopic, encodedDisk...), encodedEngine...)

	storedEntries := decodeMulti(multipleEntries)
	if string(storedEntries[0].Key) != "topic" {
		t.Fatalf("Expected decoded key to be %v, received %v", "topic", string(storedEntries[0].Key))
	}
	if string(storedEntries[0].Value) != "microservices" {
		t.Fatalf("Expected decoded value to be %v, received %v", "microservices", string(storedEntries[0].Value))
	}

	if string(storedEntries[1].Key) != "disk" {
		t.Fatalf("Expected decoded key to be %v, received %v", "disk", string(storedEntries[1].Key))
	}
	if string(storedEntries[1].Value) != "ssd" {
		t.Fatalf("Expected decoded value to be %v, received %v", "ssd", string(storedEntries[1].Value))
	}

	if string(storedEntries[2].Key) != "engine" {
		t.Fatalf("Expected decoded key to be %v, received %v", "engine", string(storedEntries[2].Key))
	}
	if string(storedEntries[2].Value) != "bitcask" {
		t.Fatalf("Expected decoded value to be %v, received %v", "bitcask", string(storedEntries[2].Value))
	}
}
