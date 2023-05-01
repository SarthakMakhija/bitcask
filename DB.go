package bitcask

import (
	"bitcask/config"
	"bitcask/kv"
	"bitcask/merge"
)

// DB is the key/value database. It contains a `KVStore` and a `MergeWorker`
// 1. KVStore is an abstraction that encapsulates append-only log segments and KeyDirectory which is an in-memory hashmap
// 2. Worker encapsulates the goroutine that performs merge and compaction of inactive segments
type DB[Key config.BitCaskKey] struct {
	kvStore *kv.KVStore[Key]
	worker  *merge.Worker[Key]
}

// NewDB takes a configuration and starts a new database instance.
func NewDB[Key config.BitCaskKey](config *config.Config[Key]) (*DB[Key], error) {
	kvStore, err := kv.NewKVStore[Key](config)
	if err != nil {
		return nil, err
	}
	return &DB[Key]{
		kvStore: kvStore,
		worker:  merge.NewWorker[Key](kvStore, config.MergeConfig()),
	}, nil
}

// Put adds a key value pair in the append-only log, followed by an entry in the hashmap inside KeyDirectory
func (db *DB[Key]) Put(key Key, value []byte) error {
	return db.kvStore.Put(key, value)
}

// Update adds a key value pair in the append-only log, followed by updating the entry in the hashmap inside KeyDirectory
// Both Update and Delete operations are append-only operations wrt log, but they are in-place update operations wrt KeyDirectory.
func (db *DB[Key]) Update(key Key, value []byte) error {
	return db.kvStore.Update(key, value)
}

// Delete adds a key value pair in the append-only log, followed by deleting the entry in the hashmap inside KeyDirectory.
// Both Update and Delete operations are append-only operations wrt log, but they are in-place update operations wrt KeyDirectory.
func (db *DB[Key]) Delete(key Key) error {
	return db.kvStore.Delete(key)
}

// SilentGet gets the value corresponding to the key. Returns value, true if the value is found, else returns nil, false
func (db *DB[Key]) SilentGet(key Key) ([]byte, bool) {
	return db.kvStore.SilentGet(key)
}

// Get gets the value corresponding to the key. Returns value, nil if the value is found, else returns nil, error
func (db *DB[Key]) Get(key Key) ([]byte, error) {
	return db.kvStore.Get(key)
}

// Shutdown performs a shutdown of the database that involves stopping the merge worker goroutine and shutting down the KVStore
func (db *DB[Key]) Shutdown() {
	db.worker.Stop()
	db.kvStore.Shutdown()
}

// Sync performs a sync of all the active and inactive segments. This implementation uses the Segment vocabulary over DataFile vocabulary
func (db *DB[Key]) Sync() {
	db.kvStore.Sync()
}

// clearLog removes all the log files
func (db *DB[Key]) clearLog() {
	db.kvStore.ClearLog()
}
