package merge

import (
	bitCaskConfig "bitcask/config"
	kv "bitcask/kv"
	"testing"
	"time"
)

func TestMergeSegmentsWithUpdate(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 8, 16, bitCaskConfig.NewMergeConfig(2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, config.MergeConfig())

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("topic", []byte("bitcask"))
	_ = store.Put("disk", []byte("ssd"))

	worker.beginMerge()

	value, _ := store.Get("topic")
	if string(value) != "bitcask" {
		t.Fatalf("Expected value to be %v, received %v", "bitcask", string(value))
	}
}

func TestMergeSegmentsWithDeletion(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 4, 16, bitCaskConfig.NewMergeConfig(2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, config.MergeConfig())

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Delete("topic")
	_ = store.Put("ssd", []byte("disk"))

	worker.beginMerge()

	value, ok := store.SilentGet("topic")
	if ok {
		t.Fatalf("Expected value to be missing for the key %v, received %v", "topic", string(value))
	}
}

func TestMergeMoreThan2Segments(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 8, 16, bitCaskConfig.NewMergeConfig(3, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	worker := NewWorker(store, config.MergeConfig())

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("disk", []byte("ssd"))
	_ = store.Put("engine", []byte("bitcask"))
	_ = store.Put("language", []byte("go"))

	worker.beginMerge()

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

func TestMergeSegmentsOnSchedule(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 8, 16, bitCaskConfig.NewMergeConfigWithAllSegmentsToReadEveryFixedDuration(1*time.Second, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	store, _ := kv.NewKVStore[serializableKey](config)
	defer store.ClearLog()

	_ = store.Put("topic", []byte("microservices"))
	_ = store.Put("topic", []byte("bitcask"))
	_ = store.Put("disk", []byte("ssd"))

	worker := NewWorker(store, config.MergeConfig())

	time.Sleep(3 * time.Second)
	worker.Stop()

	value, _ := store.Get("topic")
	if string(value) != "bitcask" {
		t.Fatalf("Expected value to be %v, received %v", "bitcask", string(value))
	}
}
