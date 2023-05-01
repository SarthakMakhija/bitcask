package kv

import (
	bitCaskConfig "bitcask/config"
	"strconv"
	"testing"
)

func TestReloadStore(t *testing.T) {
	config := bitCaskConfig.NewConfig(".", 256, 16, bitCaskConfig.NewMergeConfig(2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	kv, _ := NewKVStore[serializableKey](config)

	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		_ = kv.Put(serializableKey(countAsString), []byte(countAsString))
	}

	kv.Sync()
	kv.Shutdown()

	kv, _ = NewKVStore[serializableKey](config)
	defer kv.ClearLog()

	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		value, _ := kv.SilentGet(serializableKey(countAsString))
		if string(value) != countAsString {
			t.Fatalf("Expected value to be %v for the key %v, received %v", countAsString, countAsString, string(value))
		}
	}
}
