package keydir

import (
	"reflect"
	"testing"
)

func TestPutsAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[string](16)
	keyDirectory.Put("topic", NewEntry(1, 10, 20))

	entry, _ := keyDirectory.Get("topic")
	if !reflect.DeepEqual(NewEntry(1, 10, 20), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(1, 10, 20), entry)
	}
}

func TestUpdatesAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[string](16)
	keyDirectory.Put("topic", NewEntry(1, 10, 20))

	entry, _ := keyDirectory.Get("topic")
	if !reflect.DeepEqual(NewEntry(1, 10, 20), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(1, 10, 20), entry)
	}

	keyDirectory.Update("topic", NewEntry(2, 20, 40))
	entry, _ = keyDirectory.Get("topic")
	if !reflect.DeepEqual(NewEntry(2, 20, 40), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(2, 20, 40), entry)
	}
}

func TestGetANonExistentKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[string](16)

	entry, ok := keyDirectory.Get("non-existing")
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, but was present %v", "non-existing", entry)
	}
}
