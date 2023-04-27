package kv

import (
	"bitcask/config"
	"bitcask/keydir"
	"bitcask/log"
	"errors"
	"fmt"
)

type KVStore[Key config.BitCaskKey] struct {
	segments     *log.Segments[Key]
	keyDirectory *keydir.KeyDirectory[Key]
}

func NewKVStore[Key config.BitCaskKey](config *config.Config) (*KVStore[Key], error) {
	segments, err := log.NewSegments[Key](config.Directory(), config.MaxSegmentSizeInBytes(), config.Clock())
	if err != nil {
		return nil, err
	}
	return &KVStore[Key]{
		segments:     segments,
		keyDirectory: keydir.NewKeyDirectory[Key](config.KeyDirectoryCapacity()),
	}, nil
}

func (kv *KVStore[Key]) Put(key Key, value []byte) error {
	appendEntryResponse, err := kv.appendInLog(key, value)
	if err != nil {
		return err
	}
	kv.keyDirectory.Put(key, keydir.NewEntryFrom(appendEntryResponse))
	return nil
}

func (kv *KVStore[Key]) Update(key Key, value []byte) error {
	appendEntryResponse, err := kv.appendInLog(key, value)
	if err != nil {
		return err
	}
	kv.keyDirectory.Update(key, keydir.NewEntryFrom(appendEntryResponse))
	return nil
}

func (kv *KVStore[Key]) Delete(key Key) error {
	if _, err := kv.segments.AppendDeleted(key); err != nil {
		return err
	}
	kv.keyDirectory.Delete(key)
	return nil
}

func (kv *KVStore[Key]) SilentGet(key Key) ([]byte, bool) {
	entry, ok := kv.keyDirectory.Get(key)
	if ok {
		storedEntry, err := kv.segments.Read(entry.FileId, entry.Offset, uint64(entry.EntryLength))
		if err != nil {
			return nil, false
		}
		return storedEntry.Value, true
	}
	return nil, false
}

func (kv *KVStore[Key]) Get(key Key) ([]byte, error) {
	entry, ok := kv.keyDirectory.Get(key)
	if ok {
		storedEntry, err := kv.segments.Read(entry.FileId, entry.Offset, uint64(entry.EntryLength))
		if err != nil {
			return nil, err
		}
		return storedEntry.Value, nil
	}
	return nil, errors.New(fmt.Sprintf("Key %v does not exist", key))
}

func (kv *KVStore[Key]) ReadPairOfInactiveSegment(keyMapper func([]byte) Key) ([][]*log.MappedStoredEntry[Key], error) {
	return kv.segments.ReadPairOfInactiveSegment(keyMapper)
}

func (kv *KVStore[Key]) ClearLog() {
	kv.segments.RemoveActive()
	kv.segments.RemoveAllInactive()
}

func (kv *KVStore[Key]) appendInLog(key Key, value []byte) (*log.AppendEntryResponse, error) {
	appendEntryResponse, err := kv.segments.Append(key, value)
	if err != nil {
		return nil, err
	}
	return appendEntryResponse, nil
}
