package log

import (
	"bitcask/key"
	"encoding/binary"
	"unsafe"
)

var reservedKeySize, reservedValueSize = uint32(unsafe.Sizeof(uint32(0))), uint32(unsafe.Sizeof(uint32(0)))
var littleEndian = binary.LittleEndian

type valueReference struct {
	value   []byte
	deleted byte
}

type Entry[Key key.Serializable] struct {
	key   Key
	value valueReference
}

func NewEntry[Key key.Serializable](key Key, value []byte) *Entry[Key] {
	return &Entry[Key]{
		key:   key,
		value: valueReference{value: value, deleted: 0},
	}
}

func (entry *Entry[Key]) encode() []byte {
	serializedKey := entry.key.Serialize()
	keySize, valueSize := uint32(len(serializedKey)), uint32(len(entry.value.value))

	encoded := make([]byte, reservedKeySize+reservedValueSize+keySize+valueSize)
	var offset uint32 = 0

	littleEndian.PutUint32(encoded, keySize)
	offset = offset + reservedKeySize

	littleEndian.PutUint32(encoded[offset:], valueSize)
	offset = offset + reservedValueSize

	copy(encoded[offset:], serializedKey)
	offset = offset + keySize

	copy(encoded[offset:], entry.value.value)
	return encoded
}

func decode(content []byte) ([]byte, []byte) {
	var offset uint32 = 0
	keySize := littleEndian.Uint32(content[offset:reservedKeySize])
	offset = offset + reservedKeySize

	valueSize := littleEndian.Uint32(content[offset:])
	offset = offset + reservedValueSize

	serializedKey := content[offset : offset+keySize]
	offset = offset + keySize

	value := content[offset : offset+valueSize]
	return serializedKey, value
}
