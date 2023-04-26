package log

import "testing"

type serializableKey string

func (key serializableKey) serialize() []byte {
	return []byte(key)
}

func TestEncodesAKeyValuePair(t *testing.T) {
	entry := NewEntry[serializableKey]("topic", []byte("microservices"))
	encoded := entry.encode()

	key, value := decode(encoded)
	if string(key) != "topic" {
		t.Fatalf("Expected decoded key to be %v, received %v", "topic", string(key))
	}
	if string(value) != "microservices" {
		t.Fatalf("Expected decoded value to be %v, received %v", "microservices", string(value))
	}
}
