package bitcask

import (
	"bitcask/config"
	"bitcask/kv"
)

type DB[Key config.BitCaskKey] struct {
	kvStore *kv.KVStore[Key]
}

func NewDB[Key config.BitCaskKey](config *config.Config) (*DB[Key], error) {
	kvStore, err := kv.NewKVStore[Key](config)
	if err != nil {
		return nil, err
	}
	return &DB[Key]{
		kvStore: kvStore,
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

func (db *DB[Key]) ClearLog() {
	db.kvStore.ClearLog()
}
