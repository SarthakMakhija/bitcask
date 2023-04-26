package keydir

type KeyDirectory[Key comparable] struct {
	entryByKey map[Key]*Entry
}

func NewKeyDirectory[Key comparable](initialCapacity uint64) *KeyDirectory[Key] {
	return &KeyDirectory[Key]{
		entryByKey: make(map[Key]*Entry, initialCapacity),
	}
}

func (keyDirectory *KeyDirectory[Key]) Put(key Key, value *Entry) {
	keyDirectory.entryByKey[key] = value
}

func (keyDirectory *KeyDirectory[Key]) Update(key Key, value *Entry) {
	keyDirectory.entryByKey[key] = value
}

func (keyDirectory *KeyDirectory[Key]) Delete(key Key) {
	delete(keyDirectory.entryByKey, key)
}

func (keyDirectory *KeyDirectory[Key]) Get(key Key) (*Entry, bool) {
	value, ok := keyDirectory.entryByKey[key]
	return value, ok
}
