package log

import (
	"errors"
	"fmt"
	"os"
)

type Store struct {
	file          *os.File
	currentOffset int64
}

func NewStore(filePath string) (*Store, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		file:          file,
		currentOffset: 0,
	}, nil
}

func (store *Store) append(bytes []byte) (int64, error) {
	bytesWritten, err := store.file.Write(bytes)
	offset := store.currentOffset
	if bytesWritten < len(bytes) {
		return -1, errors.New(fmt.Sprintf("Could not append %v bytes", len(bytes)))
	}
	if err != nil {
		return -1, err
	}
	store.currentOffset = store.currentOffset + int64(bytesWritten)
	return offset, nil
}

func (store *Store) read(offset int64, size uint64) ([]byte, error) {
	_, err := store.file.Seek(offset, 0)
	if err != nil {
		return nil, err
	}
	bytes := make([]byte, size)

	_, err = store.file.Read(bytes)
	if err != nil {
		return nil, err
	}
	return bytes, nil
}

func (store *Store) sync() {
	store.file.Sync()
}

func (store *Store) remove() {
	_ = os.RemoveAll(store.file.Name())
}
