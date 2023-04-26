package keydir

import (
	"reflect"
	"testing"
)

func TestPutsAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[string](16)
	keyDirectory.Put("topic", []byte("microservices"))

	value, _ := keyDirectory.Get("topic")
	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected %v, received %v from key directory", []byte("microservices"), string(value))
	}
}

func TestGetANonExistentKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[string](16)

	value, ok := keyDirectory.Get("non-existing")
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, but was present %v", "non-existing", string(value))
	}
}
