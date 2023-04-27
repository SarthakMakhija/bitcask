package log

import (
	"bitcask/clock"
	"bitcask/key"
	"encoding/binary"
	"unsafe"
)

var reservedKeySize, reservedValueSize = uint32(unsafe.Sizeof(uint32(0))), uint32(unsafe.Sizeof(uint32(0)))
var reservedTimestampSize = uint32(unsafe.Sizeof(uint32(0)))
var littleEndian = binary.LittleEndian
var tombstoneMarkerSize = uint32(unsafe.Sizeof(byte(0)))

type valueReference struct {
	value     []byte
	tombstone byte
}

type Entry[Key key.Serializable] struct {
	key   Key
	value valueReference
	clock clock.Clock
}

func NewEntry[Key key.Serializable](key Key, value []byte, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:   key,
		value: valueReference{value: value, tombstone: 0},
		clock: clock,
	}
}

func NewDeletedEntry[Key key.Serializable](key Key, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:   key,
		value: valueReference{value: []byte{}, tombstone: 1},
		clock: clock,
	}
}

func (entry *Entry[Key]) encode() []byte {
	serializedKey := entry.key.Serialize()
	keySize, valueSize := uint32(len(serializedKey)), uint32(len(entry.value.value))+tombstoneMarkerSize

	encoded := make([]byte, reservedTimestampSize+reservedKeySize+reservedValueSize+keySize+valueSize)
	var offset uint32 = 0

	littleEndian.PutUint32(encoded, uint32(entry.clock.Now()))
	offset = offset + reservedTimestampSize

	littleEndian.PutUint32(encoded[offset:], keySize)
	offset = offset + reservedKeySize

	littleEndian.PutUint32(encoded[offset:], valueSize)
	offset = offset + reservedValueSize

	copy(encoded[offset:], serializedKey)
	offset = offset + keySize

	copy(encoded[offset:], append(entry.value.value, entry.value.tombstone))
	return encoded
}

func decode(content []byte) *StoredEntry {
	var offset uint32 = 0
	storedEntry, _ := decodeFrom(content, offset)
	return storedEntry
}

func decodeMulti(content []byte) []*StoredEntry {
	contentLength := uint32(len(content))
	var offset uint32 = 0

	var entries []*StoredEntry
	for offset < contentLength {
		entry, traversedOffset := decodeFrom(content, offset)
		entries = append(entries, entry)
		offset = traversedOffset
	}
	return entries
}

func decodeFrom(content []byte, offset uint32) (*StoredEntry, uint32) {
	_ = littleEndian.Uint32(content)
	offset = offset + reservedTimestampSize

	keySize := littleEndian.Uint32(content[offset:])
	offset = offset + reservedKeySize

	valueSize := littleEndian.Uint32(content[offset:])
	offset = offset + reservedValueSize

	serializedKey := content[offset : offset+keySize]
	offset = offset + keySize

	value := content[offset : offset+valueSize]
	offset = offset + valueSize

	valueLength := len(value)
	return &StoredEntry{
		Key:     serializedKey,
		Value:   value[:valueLength-1],
		Deleted: value[valueLength-1]&0x01 == 0x01,
	}, offset
}
