package kv

import (
	"bitcask/config"
	appendOnlyLog "bitcask/kv/log"
	"errors"
	"fmt"
	"sync"
)

type KVStore[Key config.BitCaskKey] struct {
	segments     *appendOnlyLog.Segments[Key]
	keyDirectory *KeyDirectory[Key]
	lock         sync.RWMutex
}

func NewKVStore[Key config.BitCaskKey](config *config.Config[Key]) (*KVStore[Key], error) {
	segments, err := appendOnlyLog.NewSegments[Key](config.Directory(), config.MaxSegmentSizeInBytes(), config.Clock())
	if err != nil {
		return nil, err
	}
	store := &KVStore[Key]{
		segments:     segments,
		keyDirectory: NewKeyDirectory[Key](config.KeyDirectoryCapacity()),
	}
	if err := store.reload(config); err != nil {
		return nil, err
	}
	return store, nil
}

func (kv *KVStore[Key]) Put(key Key, value []byte) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	appendEntryResponse, err := kv.segments.Append(key, value)
	if err != nil {
		return err
	}
	kv.keyDirectory.Put(key, NewEntryFrom(appendEntryResponse))
	return nil
}

func (kv *KVStore[Key]) Update(key Key, value []byte) error {
	return kv.Put(key, value)
}

func (kv *KVStore[Key]) Delete(key Key) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if _, err := kv.segments.AppendDeleted(key); err != nil {
		return err
	}
	kv.keyDirectory.Delete(key)
	return nil
}

func (kv *KVStore[Key]) SilentGet(key Key) ([]byte, bool) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	entry, ok := kv.keyDirectory.Get(key)
	if ok {
		storedEntry, err := kv.segments.Read(entry.FileId, entry.Offset, entry.EntryLength)
		if err != nil {
			return nil, false
		}
		return storedEntry.Value, true
	}
	return nil, false
}

func (kv *KVStore[Key]) Get(key Key) ([]byte, error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	entry, ok := kv.keyDirectory.Get(key)
	if ok {
		storedEntry, err := kv.segments.Read(entry.FileId, entry.Offset, entry.EntryLength)
		if err != nil {
			return nil, err
		}
		return storedEntry.Value, nil
	}
	return nil, errors.New(fmt.Sprintf("Key %v does not exist", key))
}

func (kv *KVStore[Key]) ReadInactiveSegments(totalSegments int, keyMapper func([]byte) Key) ([]uint64, [][]*appendOnlyLog.MappedStoredEntry[Key], error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	return kv.segments.ReadInactiveSegments(totalSegments, keyMapper)
}

func (kv *KVStore[Key]) ReadAllInactiveSegments(keyMapper func([]byte) Key) ([]uint64, [][]*appendOnlyLog.MappedStoredEntry[Key], error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	return kv.segments.ReadAllInactiveSegments(keyMapper)
}

func (kv *KVStore[Key]) WriteBack(fileIds []uint64, changes map[Key]*appendOnlyLog.MappedStoredEntry[Key]) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	writeBackResponses, err := kv.segments.WriteBack(changes)
	if err != nil {
		return err
	}
	kv.keyDirectory.BulkUpdate(writeBackResponses)
	kv.segments.Remove(fileIds)
	return nil
}

func (kv *KVStore[Key]) ClearLog() {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.segments.RemoveActive()
	kv.segments.RemoveAllInactive()
}

func (kv *KVStore[Key]) Sync() {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.segments.Sync()
}

func (kv *KVStore[Key]) Shutdown() {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.segments.Shutdown()
}

func (kv *KVStore[Key]) reload(cfg *config.Config[Key]) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	for fileId, segment := range kv.segments.AllInactiveSegments() {
		entries, err := segment.ReadFull(cfg.MergeConfig().KeyMapper())
		if err != nil {
			return err
		}
		kv.keyDirectory.Reload(fileId, entries)
	}
	return nil
}
