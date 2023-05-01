package kv

import (
	"bitcask/config"
	"bitcask/kv/log"
)

type KeyDirectory[Key config.BitCaskKey] struct {
	entryByKey map[Key]*Entry
}

func NewKeyDirectory[Key config.BitCaskKey](initialCapacity uint64) *KeyDirectory[Key] {
	return &KeyDirectory[Key]{
		entryByKey: make(map[Key]*Entry, initialCapacity),
	}
}

func (keyDirectory *KeyDirectory[Key]) Reload(fileId uint64, entries []*log.MappedStoredEntry[Key]) {
	for _, entry := range entries {
		keyDirectory.entryByKey[entry.Key] = NewEntry(fileId, int64(entry.KeyOffset), entry.EntryLength)
	}
}

func (keyDirectory *KeyDirectory[Key]) Put(key Key, value *Entry) {
	keyDirectory.entryByKey[key] = value
}

func (keyDirectory *KeyDirectory[Key]) BulkUpdate(changes []*log.WriteBackResponse[Key]) {
	for _, change := range changes {
		keyDirectory.entryByKey[change.Key] = NewEntryFrom(change.AppendEntryResponse)
	}
}

func (keyDirectory *KeyDirectory[Key]) Delete(key Key) {
	delete(keyDirectory.entryByKey, key)
}

func (keyDirectory *KeyDirectory[Key]) Get(key Key) (*Entry, bool) {
	value, ok := keyDirectory.entryByKey[key]
	return value, ok
}
