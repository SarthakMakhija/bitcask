package kv

import (
	log2 "bitcask/kv/log"
	"reflect"
	"testing"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestPutsAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey](16)
	keyDirectory.Put("topic", NewEntry(1, 10, 20))

	entry, _ := keyDirectory.Get("topic")
	if !reflect.DeepEqual(NewEntry(1, 10, 20), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(1, 10, 20), entry)
	}
}

func TestDeletesAKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey](16)
	keyDirectory.Put("topic", NewEntry(1, 10, 20))

	entry, _ := keyDirectory.Get("topic")
	if !reflect.DeepEqual(NewEntry(1, 10, 20), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(1, 10, 20), entry)
	}

	keyDirectory.Delete("topic")
	_, ok := keyDirectory.Get("topic")
	if ok {
		t.Fatalf("Expected the key %v to have been deleted but was not", "topic")
	}
}

func TestGetANonExistentKeyInKeyDirectory(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey](16)

	entry, ok := keyDirectory.Get("non-existing")
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, but was present %v", "non-existing", entry)
	}
}

func TestBulkUpdatesKeys(t *testing.T) {
	keyDirectory := NewKeyDirectory[serializableKey](16)
	response := &log2.WriteBackResponse[serializableKey]{
		Key: "topic",
		AppendEntryResponse: &log2.AppendEntryResponse{
			FileId:      10,
			Offset:      30,
			EntryLength: 36,
		},
	}
	otherResponse := &log2.WriteBackResponse[serializableKey]{
		Key: "disk",
		AppendEntryResponse: &log2.AppendEntryResponse{
			FileId:      20,
			Offset:      40,
			EntryLength: 46,
		},
	}

	keyDirectory.BulkUpdate([]*log2.WriteBackResponse[serializableKey]{response, otherResponse})

	entry, _ := keyDirectory.Get("topic")
	if !reflect.DeepEqual(NewEntry(10, 30, 36), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(10, 30, 36), entry)
	}
	entry, _ = keyDirectory.Get("disk")
	if !reflect.DeepEqual(NewEntry(20, 40, 46), entry) {
		t.Fatalf("Expected %v, received %v from key directory", NewEntry(20, 40, 46), entry)
	}
}
