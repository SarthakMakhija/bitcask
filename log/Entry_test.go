package log

import "testing"

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestEncodesAKeyValuePair(t *testing.T) {
	entry := NewEntry[serializableKey]("topic", []byte("microservices"))
	encoded := entry.encode()

	key, value, deleted := decode(encoded)
	if deleted {
		t.Fatalf("Expected key to not be deleted, but was deleted")
	}
	if string(key) != "topic" {
		t.Fatalf("Expected decoded key to be %v, received %v", "topic", string(key))
	}
	if string(value) != "microservices" {
		t.Fatalf("Expected decoded value to be %v, received %v", "microservices", string(value))
	}
}

func TestEncodesADeletedKeyValuePair(t *testing.T) {
	entry := NewDeletedEntry[serializableKey]("topic")
	encoded := entry.encode()

	key, _, deleted := decode(encoded)
	if !deleted {
		t.Fatalf("Expected key to be deleted, but was not")
	}
	if string(key) != "topic" {
		t.Fatalf("Expected decoded key to be %v, received %v", "topic", string(key))
	}
}
