package kv

import (
	"bitcask/config"
	appendOnlyLog "bitcask/kv/log"
	"errors"
	"fmt"
	"sync"
)

// KVStore encapsulates append-only log segments and KeyDirectory which is an in-memory hashmap
// Segments is an abstraction that manages the active and K inactive segments.
// KVStore also maintains a RWLock that allows an exclusive writer and N readers
type KVStore[Key config.BitCaskKey] struct {
	segments     *appendOnlyLog.Segments[Key]
	keyDirectory *KeyDirectory[Key]
	lock         sync.RWMutex
}

// NewKVStore creates a new instance of KVStore
// It also performs a reload operation `store.reload(config)` that is responsible for reloading the state of KeyDirectory from inactive segments
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

// Put puts the key and the value in bitcask. Put operations consists of the following steps:
// 1.Append the key and the value in the append-only active segment using `kv.segments.Append(key, value)`.
// - Segments abstraction will append the key and the value to the active segment if the size of the active segment is less than the threshold, else it will perform a rollover of the active segment
// 2.Once the append operation is successful, it will write the key and the Entry to the KeyDirectory, which is an in-memory representation of the key and its position in an append-only segment
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

// Update is very much similar to Put. It appends the key and the value to the log and performs an in-place update in the KeyDirectory
func (kv *KVStore[Key]) Update(key Key, value []byte) error {
	return kv.Put(key, value)
}

// Delete appends the key and the value to the log and performs an in-place delete in the KeyDirectory
func (kv *KVStore[Key]) Delete(key Key) error {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	if _, err := kv.segments.AppendDeleted(key); err != nil {
		return err
	}
	kv.keyDirectory.Delete(key)
	return nil
}

// SilentGet Gets the value corresponding to the key. Returns value and true if the value is found, else returns nil and false
// In order to perform SilentGet, a Get operation is performed in the KeyDirectory which returns an Entry indicating the fileId containing the key, offset of the key and the entry length
// If an Entry corresponding to the key is found, a Read operation is performed in the Segments abstraction, which performs an in-memory lookup to identify the segment based on the fileId, and then a Read operation is performed in that Segment
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

// Get gets the value corresponding to the key. Returns value and nil if the value is found, else returns nil and error
// In order to perform Get, a Get operation is performed in the KeyDirectory which returns an Entry indicating the fileId, offset of the key and the entry length
// If an Entry corresponding to the key is found, a Read operation is performed in the Segments abstraction, which performs an in-memory lookup to identify the segment based on the fileId, and then a Read operation is performed in that Segment
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

// ReadInactiveSegments reads inactive segments identified by `totalSegments`. This operation is performed during merge.
// keyMapper is used to map a byte slice Key to a generically typed Key. keyMapper is basically a means to perform deserialization of keys which is necessary to update the state in KeyDirectory after the merge operation is done, more on this is mentioned in KeyDirectory.go
func (kv *KVStore[Key]) ReadInactiveSegments(totalSegments int, keyMapper func([]byte) Key) ([]uint64, [][]*appendOnlyLog.MappedStoredEntry[Key], error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	return kv.segments.ReadInactiveSegments(totalSegments, keyMapper)
}

// ReadAllInactiveSegments reads all the inactive segments. This operation is performed during merge.
// keyMapper is used to map a byte slice Key to a generically typed Key. keyMapper is basically a means to perform deserialization of keys which is necessary to update the state in KeyDirectory after the merge operation is done, more on this is mentioned in KeyDirectory.go and Worker.go inside merge/ package.
func (kv *KVStore[Key]) ReadAllInactiveSegments(keyMapper func([]byte) Key) ([]uint64, [][]*appendOnlyLog.MappedStoredEntry[Key], error) {
	kv.lock.RLock()
	defer kv.lock.RUnlock()

	return kv.segments.ReadAllInactiveSegments(keyMapper)
}

// WriteBack writes back the changes (merged changes) to new inactive segments. This operation is performed during merge.
// It writes all the changes into M new inactive segments and once those changes are written to the new inactive segment(s), the state of the keys present in the `changes` parameter is updated in the KeyDirectory. More on this is mentioned in Worker.go inside merge/ package.
// Once the state is updated in the KeyDirectory, the old segments identified by `fileIds` are removed from disk.
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

// ClearLog removes all the log files
func (kv *KVStore[Key]) ClearLog() {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.segments.RemoveActive()
	kv.segments.RemoveAllInactive()
}

// Sync performs a sync of all the active and inactive segments. This implementation uses the Segment vocabulary over DataFile vocabulary
func (kv *KVStore[Key]) Sync() {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.segments.Sync()
}

// Shutdown performs a shutdown of the segments which involves setting the active segment to nil and removing the entire in-memory representation of the inactive segments
func (kv *KVStore[Key]) Shutdown() {
	kv.lock.Lock()
	defer kv.lock.Unlock()

	kv.segments.Shutdown()
}

// reload the entire state during start-up.
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
