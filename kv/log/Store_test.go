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
	_, _ = store.append([]byte(content))

	bytes, _ := store.read(0, uint32(len(content)))

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

	_, _ = store.append([]byte(contentAppendOnly))
	_, _ = store.append([]byte(contentStorage))

	bytes, _ := store.read(int64(len(contentAppendOnly)), uint32(len(contentStorage)))

	if string(bytes) != contentStorage {
		t.Fatalf("Expected store content to be %v, received %v", contentStorage, string(bytes))
	}
}

func TestAppendsMultipleEntriesToTheStoreAndValidatesOffset(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	contentAppendOnly := "append-only-log"
	contentStorage := "storage"

	offset, _ := store.append([]byte(contentAppendOnly))
	anotherOffset, _ := store.append([]byte(contentStorage))

	if offset != 0 {
		t.Fatalf("Expected initial offset to be %v, received %v", 0, offset)
	}
	if anotherOffset != 15 {
		t.Fatalf("Expected another offset to be %v, received %v", 15, offset)
	}
}

func TestAppendsToTheStoreAndPerformsSync(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	content := "append-only-log"
	_, _ = store.append([]byte(content))

	store.sync()

	bytes, _ := store.read(0, uint32(len(content)))

	if string(bytes) != content {
		t.Fatalf("Expected store content to be %v, received %v", content, string(bytes))
	}
}

func TestAppendsMultipleEntriesToTheStoreAndValidatesSize(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	contentAppendOnly := "append-only-log"
	contentStorage := "storage"

	_, _ = store.append([]byte(contentAppendOnly))
	_, _ = store.append([]byte(contentStorage))

	if store.sizeInBytes() != int64(len(contentAppendOnly)+len(contentStorage)) {
		t.Fatalf("Expected store sizeInBytes to be %v, received %v", len(contentAppendOnly)+len(contentStorage), store.sizeInBytes())
	}
}

func TestReadsTheCompleteFile(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	contentAppendOnly := "append-only-log"
	contentStorage := "storage"

	_, _ = store.append([]byte(contentAppendOnly))
	_, _ = store.append([]byte(contentStorage))

	contents, _ := store.readFull()
	if string(contents[:len(contentAppendOnly)]) != contentAppendOnly {
		t.Fatalf("Expected content initial part to be %v, received %v", contentAppendOnly, string(contents[:len(contentAppendOnly)]))
	}
	if string(contents[len(contentAppendOnly):]) != contentStorage {
		t.Fatalf("Expected the remaining part to be %v, received %v", contentStorage, string(contents[len(contentAppendOnly):]))
	}
}

func TestStopsWrites(t *testing.T) {
	file, _ := os.CreateTemp(".", "append_only")
	store, _ := NewStore(file.Name())
	defer func() {
		_ = os.RemoveAll(file.Name())
	}()

	content := "append-only-log"
	_, _ = store.append([]byte(content))

	store.stopWrites()

	_, err := store.append([]byte("stop-writes"))
	if err == nil {
		t.Fatalf("Expected error while writing to store after it was write closed but no error was received")
	}

	received, _ := store.readFull()
	if string(received) != content {
		t.Fatalf("Expected content to be %v, received %v", content, string(received))
	}
}
