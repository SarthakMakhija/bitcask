package kv

import (
	bitCaskConfig "bitcask/config"
	"reflect"
	"strconv"
	"sync"
	"testing"
)

func TestPutConcurrently(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 32, 16, bitCaskConfig.NewMergeConfig(2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	var wg sync.WaitGroup
	wg.Add(3)

	go func() {
		defer wg.Done()
		_ = kv.Put("topic", []byte("microservices"))
	}()
	go func() {
		defer wg.Done()
		_ = kv.Put("disk", []byte("ssd"))
	}()
	go func() {
		defer wg.Done()
		_ = kv.Put("storage", []byte("bitcask"))
	}()

	wg.Wait()

	value, _ := kv.SilentGet("topic")
	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
	value, _ = kv.SilentGet("disk")
	if !reflect.DeepEqual([]byte("ssd"), value) {
		t.Fatalf("Expected value to be %v, received %v", "ssd", string(value))
	}
	value, _ = kv.SilentGet("storage")
	if !reflect.DeepEqual([]byte("bitcask"), value) {
		t.Fatalf("Expected value to be %v, received %v", "bitcask", string(value))
	}
}

func TestPutConcurrentlyAcrossManyGoroutines(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 10124, 16, bitCaskConfig.NewMergeConfig(2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	kv, _ := NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	var wg sync.WaitGroup
	wg.Add(100)
	for count := 1; count <= 100; count++ {
		go func(count int) {
			defer wg.Done()
			countAsString := strconv.Itoa(count)
			_ = kv.Put(serializableKey(countAsString), []byte(countAsString))
		}(count)
	}

	wg.Wait()
	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		value, _ := kv.SilentGet(serializableKey(countAsString))
		if string(value) != countAsString {
			t.Fatalf("Expected value to be %v for the key %v, received %v", countAsString, countAsString, string(value))
		}
	}
}
