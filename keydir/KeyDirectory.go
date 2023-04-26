package keydir

type KeyDirectory[Key comparable] struct {
	valueByKey map[Key][]byte
}

func NewKeyDirectory[Key comparable](initialCapacity uint64) *KeyDirectory[Key] {
	return &KeyDirectory[Key]{
		valueByKey: make(map[Key][]byte, initialCapacity),
	}
}

func (keyDirectory *KeyDirectory[Key]) Put(key Key, value []byte) {
	keyDirectory.valueByKey[key] = value
}

func (keyDirectory *KeyDirectory[Key]) Get(key Key) ([]byte, bool) {
	value, ok := keyDirectory.valueByKey[key]
	return value, ok
}
