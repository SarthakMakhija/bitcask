package bitcask

import (
	"bitcask/key"
	"bitcask/keydir"
	"bitcask/log"
	"errors"
	"fmt"
)

type DB[Key key.BitCaskKey] struct {
	segments     *log.Segments[Key]
	keyDirectory *keydir.KeyDirectory[Key]
}

func NewDB[Key key.BitCaskKey](config *Config) (*DB[Key], error) {
	segments, err := log.NewSegments[Key](config.Directory(), config.MaxSegmentSizeInBytes())
	if err != nil {
		return nil, err
	}
	return &DB[Key]{
		segments:     segments,
		keyDirectory: keydir.NewKeyDirectory[Key](config.KeyDirectoryCapacity()),
	}, nil
}

func (db *DB[Key]) Put(key Key, value []byte) error {
	appendEntryResponse, err := db.appendInLog(key, value)
	if err != nil {
		return err
	}
	db.keyDirectory.Put(key, keydir.NewEntryFrom(appendEntryResponse))
	return nil
}

func (db *DB[Key]) Update(key Key, value []byte) error {
	appendEntryResponse, err := db.appendInLog(key, value)
	if err != nil {
		return err
	}
	db.keyDirectory.Update(key, keydir.NewEntryFrom(appendEntryResponse))
	return nil
}

func (db *DB[Key]) SilentGet(key Key) ([]byte, bool) {
	entry, ok := db.keyDirectory.Get(key)
	if ok {
		storedEntry, err := db.segments.Read(entry.FileId, entry.Offset, uint64(entry.EntryLength))
		if err != nil {
			return nil, false
		}
		return storedEntry.Value, true
	}
	return nil, false
}

func (db *DB[Key]) Get(key Key) ([]byte, error) {
	entry, ok := db.keyDirectory.Get(key)
	if ok {
		storedEntry, err := db.segments.Read(entry.FileId, entry.Offset, uint64(entry.EntryLength))
		if err != nil {
			return nil, err
		}
		return storedEntry.Value, nil
	}
	return nil, errors.New(fmt.Sprintf("Key %v does not exist", key))
}

func (db *DB[Key]) ClearLog() {
	db.segments.RemoveActive()
	db.segments.RemoveAllInactive()
}

func (db *DB[Key]) appendInLog(key Key, value []byte) (*log.AppendEntryResponse, error) {
	appendEntryResponse, err := db.segments.Append(key, value)
	if err != nil {
		return nil, err
	}
	return appendEntryResponse, nil
}
