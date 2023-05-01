package bitcask

import (
	"bitcask/config"
	"reflect"
	"strconv"
	"testing"
)

type serializableKey string

func (key serializableKey) Serialize() []byte {
	return []byte(key)
}

func TestPutAndDoASilentGet(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_ = db.Put("topic", []byte("microservices"))

	value, _ := db.SilentGet("topic")

	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
}

func TestSilentGetANonExistentKey(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_, exists := db.SilentGet("non-existing")

	if exists {
		t.Fatalf("Expected %v to not exist but was found in the database", "non-existing")
	}
}

func TestPutAndDoAGet(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_ = db.Put("topic", []byte("microservices"))

	value, _ := db.Get("topic")

	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
}

func TestGetANonExistentKey(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	value, err := db.Get("non-existing")

	if err == nil {
		t.Fatalf("Expected %v to not exist but was found in the database with value %v", "non-existing", string(value))
	}
}

func TestUpdateAndDoASilentGet(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Update("topic", []byte("storage engine"))

	value, _ := db.SilentGet("topic")

	if !reflect.DeepEqual([]byte("storage engine"), value) {
		t.Fatalf("Expected value to be %v, received %v", "storage engine", string(value))
	}
}

func TestUpdateAndDoAGet(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Update("topic", []byte("storage engine"))

	value, _ := db.Get("topic")

	if !reflect.DeepEqual([]byte("storage engine"), value) {
		t.Fatalf("Expected value to be %v, received %v", "storage engine", string(value))
	}
}

func TestDeleteAndDoASilentGet(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Delete("topic")

	_, exists := db.SilentGet("topic")
	if exists {
		t.Fatalf("Expected %v to have been deleted but was found in the database", "topic")
	}
}

func TestDeleteAndDoAGet(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)
	defer db.Shutdown()
	defer db.clearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Delete("topic")

	_, err := db.Get("topic")
	if err == nil {
		t.Fatalf("Expected %v to have been deleted but was found in the database", "topic")
	}
}

func TestReloadDB(t *testing.T) {
	cfg := config.NewConfig[serializableKey](".", 32, 16, config.NewMergeConfig[serializableKey](2, func(key []byte) serializableKey {
		return serializableKey(key)
	}))
	db, _ := NewDB[serializableKey](cfg)

	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		_ = db.Put(serializableKey(countAsString), []byte(countAsString))
	}

	db.Sync()
	db.Shutdown()

	db, _ = NewDB[serializableKey](cfg)
	defer db.clearLog()

	for count := 1; count <= 100; count++ {
		countAsString := strconv.Itoa(count)
		value, _ := db.SilentGet(serializableKey(countAsString))
		if string(value) != countAsString {
			t.Fatalf("Expected value to be %v for the key %v, received %v", countAsString, countAsString, string(value))
		}
	}
}
