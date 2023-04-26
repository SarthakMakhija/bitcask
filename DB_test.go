package bitcask

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
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_ = db.Put("topic", []byte("microservices"))

	value, _ := db.SilentGet("topic")

	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
}

func TestSilentGetANonExistentKey(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_, exists := db.SilentGet("non-existing")

	if exists {
		t.Fatalf("Expected %v to not exist but was found in the database", "non-existing")
	}
}

func TestPutAndDoAGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_ = db.Put("topic", []byte("microservices"))

	value, _ := db.Get("topic")

	if !reflect.DeepEqual([]byte("microservices"), value) {
		t.Fatalf("Expected value to be %v, received %v", "microservices", string(value))
	}
}

func TestGetANonExistentKey(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	value, err := db.Get("non-existing")

	if err == nil {
		t.Fatalf("Expected %v to not exist but was found in the database with value %v", "non-existing", string(value))
	}
}

func TestUpdateAndDoASilentGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Update("topic", []byte("storage engine"))

	value, _ := db.SilentGet("topic")

	if !reflect.DeepEqual([]byte("storage engine"), value) {
		t.Fatalf("Expected value to be %v, received %v", "storage engine", string(value))
	}
}

func TestUpdateAndDoAGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Update("topic", []byte("storage engine"))

	value, _ := db.Get("topic")

	if !reflect.DeepEqual([]byte("storage engine"), value) {
		t.Fatalf("Expected value to be %v, received %v", "storage engine", string(value))
	}
}

func TestDeleteAndDoASilentGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Delete("topic")

	_, exists := db.SilentGet("topic")
	if exists {
		t.Fatalf("Expected %v to have been deleted but was found in the database", "topic")
	}
}

func TestDeleteAndDoAGet(t *testing.T) {
	config := NewConfig(".", 32, 16)
	db, _ := NewDB[serializableKey](config)
	defer db.ClearLog()

	_ = db.Put("topic", []byte("microservices"))
	_ = db.Delete("topic")

	_, err := db.Get("topic")
	if err == nil {
		t.Fatalf("Expected %v to have been deleted but was found in the database", "topic")
	}
}
