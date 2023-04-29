package log

import (
	"errors"
	"fmt"
	"os"
)

type Store struct {
	writer        *os.File
	reader        *os.File
	currentOffset int64
}

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
		writer:        writer,
		reader:        reader,
		currentOffset: 0,
	}, nil
}

func (store *Store) append(bytes []byte) (int64, error) {
	bytesWritten, err := store.writer.Write(bytes)
	offset := store.currentOffset
	if err != nil {
		return -1, err
	}
	if bytesWritten < len(bytes) {
		return -1, errors.New(fmt.Sprintf("Could not append %v bytes", len(bytes)))
	}
	store.currentOffset = store.currentOffset + int64(bytesWritten)
	return offset, nil
}

func (store *Store) read(offset int64, size uint64) ([]byte, error) {
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

func (store *Store) readFull() ([]byte, error) {
	return os.ReadFile(store.reader.Name())
}

func (store *Store) sizeInBytes() int64 {
	return store.currentOffset
}

func (store *Store) sync() {
	store.writer.Sync()
}

func (store *Store) stopWrites() {
	store.writer.Close()
}

func (store *Store) remove() {
	_ = os.RemoveAll(store.writer.Name())
}
