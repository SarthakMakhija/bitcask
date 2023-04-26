package log

import (
	"encoding/binary"
	"unsafe"
)

var reservedKeySize, reservedValueSize = uint32(unsafe.Sizeof(uint32(0))), uint32(unsafe.Sizeof(uint32(0)))
var littleEndian = binary.LittleEndian

type Serializable interface {
	serialize() []byte
}

type Entry[Key Serializable] struct {
	key   Key
	value []byte
}

func NewEntry[Key Serializable](key Key, value []byte) *Entry[Key] {
	return &Entry[Key]{
		key:   key,
		value: value,
	}
}

func (entry *Entry[Key]) encode() []byte {
	key := entry.key.serialize()
	keySize, valueSize := uint32(len(key)), uint32(len(entry.value))

	encoded := make([]byte, reservedKeySize+reservedValueSize+keySize+valueSize)
	var offset uint32 = 0

	littleEndian.PutUint32(encoded, keySize)
	offset = offset + reservedKeySize

	littleEndian.PutUint32(encoded[offset:], valueSize)
	offset = offset + reservedValueSize

	copy(encoded[offset:], key)
	offset = offset + keySize

	copy(encoded[offset:], entry.value)
	return encoded
}

func decode(content []byte) ([]byte, []byte) {
	var offset uint32 = 0
	keySize := littleEndian.Uint32(content[offset:reservedKeySize])
	offset = offset + reservedKeySize

	valueSize := littleEndian.Uint32(content[offset:])
	offset = offset + reservedValueSize

	key := content[offset : offset+keySize]
	offset = offset + keySize

	value := content[offset : offset+valueSize]
	return key, value
}
