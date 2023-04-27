package kv

import (
	"reflect"
	"testing"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestPutAndDoASilentGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))

	value, _ := kv.SilentGet("topic")

	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
}

func TestSilentGetANonExistentKey(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_, exists := kv.SilentGet("non-existing")

	if exists {
		t.Fatalf("Expected %v to not exist but was found in the database", "non-existing")
	}
}

func TestPutAndDoAGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))

	value, _ := kv.Get("topic")

	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
}

func TestGetANonExistentKey(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	value, err := kv.Get("non-existing")

	if err == nil {
		t.Fatalf("Expected %v to not exist but was found in the database with value %v", "non-existing", string(value))
	}
}

func TestUpdateAndDoASilentGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))
	_ = kv.Update("topic", []byte("storage engine"))

	value, _ := kv.SilentGet("topic")

	if !reflect.DeepEqual([]byte("storage engine"), value) {
		t.Fatalf("Expected value to be %v, received %v", "storage engine", string(value))
	}
}

func TestUpdateAndDoAGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))
	_ = kv.Update("topic", []byte("storage engine"))

	value, _ := kv.Get("topic")

	if !reflect.DeepEqual([]byte("storage engine"), value) {
		t.Fatalf("Expected value to be %v, received %v", "storage engine", string(value))
	}
}

func TestDeleteAndDoASilentGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))
	_ = kv.Delete("topic")

	_, exists := kv.SilentGet("topic")
	if exists {
		t.Fatalf("Expected %v to have been deleted but was found in the database", "topic")
	}
}

func TestDeleteAndDoAGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))
	_ = kv.Delete("topic")

	_, err := kv.Get("topic")
	if err == nil {
		t.Fatalf("Expected %v to have been deleted but was found in the database", "topic")
	}
}

func TestReadsAPairOfInactiveSegments(t *testing.T) {
	config := NewConfig(".", 8, 16)
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	_ = kv.Put("topic", []byte("microservices"))
	_ = kv.Put("diskType", []byte("solid state drive"))
	_ = kv.Put("engine", []byte("bitcask"))

	pair, _ := kv.ReadPairOfInactiveSegment(func(key []byte) serializableKey {
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