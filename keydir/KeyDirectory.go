package keydir

type KeyDirectory[Key comparable] struct {
	valueByKey map[Key]*Entry
}

func NewKeyDirectory[Key comparable](initialCapacity uint64) *KeyDirectory[Key] {
	return &KeyDirectory[Key]{
		valueByKey: make(map[Key]*Entry, initialCapacity),
	}
}

func (keyDirectory *KeyDirectory[Key]) Put(key Key, value *Entry) {
	keyDirectory.valueByKey[key] = value
}

func (keyDirectory *KeyDirectory[Key]) Update(key Key, value *Entry) {
	keyDirectory.valueByKey[key] = value
}

func (keyDirectory *KeyDirectory[Key]) Get(key Key) (*Entry, bool) {
	value, ok := keyDirectory.valueByKey[key]
	return value, ok
}
