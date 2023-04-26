package log

import (
	"os"
	"testing"
)

func TestAppendsToTheStore(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	content := "append-only-log"
	_ = store.append([]byte(content))

	bytes, _ := store.read(0, uint64(len(content)))

	if string(bytes) != content {
		t.Fatalf("Expected store content to be %v, received %v", content, string(bytes))
	}
}

func TestAppendsMultipleEntriesToTheStore(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	contentAppendOnly := "append-only-log"
	contentStorage := "storage"

	_ = store.append([]byte(contentAppendOnly))
	_ = store.append([]byte(contentStorage))

	bytes, _ := store.read(int64(len(contentAppendOnly)), uint64(len(contentStorage)))

	if string(bytes) != contentStorage {
		t.Fatalf("Expected store content to be %v, received %v", contentStorage, string(bytes))
	}
}
