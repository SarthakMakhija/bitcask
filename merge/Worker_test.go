package merge

import (
	bitCaskConfig "bitcask/config"
	kv "bitcask/kv"
	"testing"
)

func TestMergeSegmentsWithUpdate(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 8, 16)
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, 2, func(key []byte) serializableKey {
		return serializableKey(key)
	})

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("topic", []byte("bitcask"))
	_ = store.Put("disk", []byte("ssd"))

	worker.begin()

	value, _ := store.Get("topic")
	if string(value) != "bitcask" {
		t.Fatalf("Expected value to be %v, received %v", "bitcask", string(value))
	}
}

func TestMergeSegmentsWithDeletion(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 4, 16)
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, 2, func(key []byte) serializableKey {
		return serializableKey(key)
	})

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Delete("topic")
	_ = store.Put("ssd", []byte("disk"))

	worker.begin()

	value, ok := store.SilentGet("topic")
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, received %v", "topic", string(value))
	}
}

func TestMergeMoreThan2Segments(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 8, 16)
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, 3, func(key []byte) serializableKey {
		return serializableKey(key)
	})

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("disk", []byte("ssd"))
	_ = store.Put("engine", []byte("bitcask"))
	_ = store.Put("language", []byte("go"))

	worker.begin()

	value, _ := store.Get("topic")
	if string(value) != "microservices" {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
	value, _ = store.Get("disk")
	if string(value) != "ssd" {
		t.Fatalf("Expected value to be %v, received %v", "ssd", string(value))
	}
	value, _ = store.Get("engine")
	if string(value) != "bitcask" {
		t.Fatalf("Expected value to be %v, received %v", "bitcask", string(value))
	}
}
