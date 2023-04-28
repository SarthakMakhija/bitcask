package merge

import (
	bitCaskConfig "bitcask/config"
	kv "bitcask/kv"
	"testing"
)

func TestMergeSegmentsWithUpdate(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 4, 16)
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, func(key []byte) serializableKey {
		return serializableKey(key)
	})

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("topic", []byte("bitcask"))

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

	worker := NewWorker(store, func(key []byte) serializableKey {
		return serializableKey(key)
	})

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Delete("topic")

	worker.begin()

	value, ok := store.SilentGet("topic")
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, received %v", "topic", string(value))
	}
}
