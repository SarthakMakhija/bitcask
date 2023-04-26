package log

import (
	"errors"
	"fmt"
	"os"
)

type Store struct {
	file *os.File
}

func NewStore(filePath string) (*Store, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}
	return &Store{
		file: file,
	}, nil
}

func (store *Store) append(bytes []byte) error {
	n, err := store.file.Write(bytes)
	if n < len(bytes) {
		return errors.New(fmt.Sprintf("Could not append %v bytes", len(bytes)))
	}
	return err
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
