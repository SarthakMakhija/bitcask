package log

import (
	"errors"
	"fmt"
	"os"
)

//Store is an abstraction that encapsulates `append`, `read`, `remove` and `sync` file operations
type Store struct {
	writer             *os.File
	reader             *os.File
	currentWriteOffset int64
}

//NewStore creates an instance of Store from the filePath. It creates 2 file pointers:
//one for writing and other for reading. The reason for creating 2 file pointers is to let kernel
//perform the necessary optimizations like block prefetch while performing writes in the append-only mode.
//Read on the other handle is very much a random disk operation.
//This implementation "NEVER" closes the read file pointer, whereas the write file pointer is closed when the active segment has reached its size threshold.
//The advantage of not closing the read file pointer is the "reduced latency" (time saved in not invoking file.open) when performing a read from the inactive segment and the
//disadvantage is that it can very well result in too many open file descriptors (FDs) on the OS level.
func NewStore(filePath string) (*Store, error) {
	writer, err := os.OpenFile(filePath, os.O_WRONLY|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		writer:             writer,
		reader:             reader,
		currentWriteOffset: 0,
	}, nil
}

//ReloadStore creates an instance of Store with only the read file pointer. This operation is executed only during the start-up to reload the state, if any from disk.
//This method creates only the read file pointer because reloading the state will only create inactive segment(s) and these will be used only for Get operation
func ReloadStore(filePath string) (*Store, error) {
	reader, err := os.OpenFile(filePath, os.O_RDONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		writer:             nil,
		reader:             reader,
		currentWriteOffset: 0,
	}, nil
}

//append Appends the bytes to the file and maintains the currentWriteOffset
func (store *Store) append(bytes []byte) (int64, error) {
	bytesWritten, err := store.writer.Write(bytes)
	offset := store.currentWriteOffset
	if err != nil {
		return -1, err
	}
	if bytesWritten < len(bytes) {
		return -1, errors.New(fmt.Sprintf("Could not append %v bytes", len(bytes)))
	}
	store.currentWriteOffset = store.currentWriteOffset + int64(bytesWritten)
	return offset, nil
}

//read Reads the file content as a byte slice from the offset. This method internally performs 2 operations:
//1. Seek to the offset
//2. Reading the byte slice of size from the offset
func (store *Store) read(offset int64, size uint32) ([]byte, error) {
	_, err := store.reader.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, size)

	_, err = store.reader.Read(bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

//readFull Reads the entire file content
func (store *Store) readFull() ([]byte, error) {
	return os.ReadFile(store.reader.Name())
}

//sizeInBytes Returns the file size in bytes. We could have used `os.Stat()` as well
func (store *Store) sizeInBytes() int64 {
	return store.currentWriteOffset
}

//sync Performs a file sync, ensures all the disk blocks (or pages) at the Kernel page cache are flushed to the disk
func (store *Store) sync() {
	store.writer.Sync()
}

//stopWrites Closes the write file pointer. This operation is called when the active segment has reached its size threshold.
func (store *Store) stopWrites() {
	store.writer.Close()
}

//remove Removes the file
func (store *Store) remove() {
	_ = os.RemoveAll(store.reader.Name())
}
