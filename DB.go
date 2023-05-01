package bitcask

import (
	"bitcask/config"
	"bitcask/kv"
	"bitcask/merge"
)

type DB[Key config.BitCaskKey] struct {
	kvStore *kv.KVStore[Key]
	worker  *merge.Worker[Key]
}

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

func (db *DB[Key]) Put(key Key, value []byte) error {
	return db.kvStore.Put(key, value)
}

func (db *DB[Key]) Update(key Key, value []byte) error {
	return db.kvStore.Update(key, value)
}

func (db *DB[Key]) Delete(key Key) error {
	return db.kvStore.Delete(key)
}

func (db *DB[Key]) SilentGet(key Key) ([]byte, bool) {
	return db.kvStore.SilentGet(key)
}

func (db *DB[Key]) Get(key Key) ([]byte, error) {
	return db.kvStore.Get(key)
}

func (db *DB[Key]) Shutdown() {
	db.worker.Stop()
	db.kvStore.Shutdown()
}

func (db *DB[Key]) Sync() {
	db.kvStore.Sync()
}

func (db *DB[Key]) clearLog() {
	db.kvStore.ClearLog()
}
