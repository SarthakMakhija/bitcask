package log

import (
	"bitcask/clock"
	"bitcask/config"
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

type Entry[Key config.Serializable] struct {
	key       Key
	value     valueReference
	timestamp uint32
	clock     clock.Clock
}

// NewEntry creates a new instance of Entry with tombstone byte set to 0 (0000 0000)
func NewEntry[Key config.Serializable](key Key, value []byte, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:       key,
		value:     valueReference{value: value, tombstone: 0},
		timestamp: 0,
		clock:     clock,
	}
}

// NewEntryPreservingTimestamp creates a new instance of Entry with tombstone byte set to 0 (0000 0000) and keeping the provided timestamp
func NewEntryPreservingTimestamp[Key config.Serializable](key Key, value []byte, ts uint32, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:       key,
		value:     valueReference{value: value, tombstone: 0},
		timestamp: ts,
		clock:     clock,
	}
}

// NewDeletedEntry creates a new instance of Entry with tombstone byte set to 1 (0000 0001)
func NewDeletedEntry[Key config.Serializable](key Key, clock clock.Clock) *Entry[Key] {
	return &Entry[Key]{
		key:       key,
		value:     valueReference{value: []byte{}, tombstone: 1},
		timestamp: 0,
		clock:     clock,
	}
}

// encode performs the encode operation which converts the Entry to a byte slice which can be written to the disk
// Encoding scheme consists of the following structure:
//
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ timestamp │ key_size │ value_size │ key │ value │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//
// timestamp, key_size, value_size consist of 32 bits each. The value ([]byte) consists of the value provided by the user and a byte for tombstone, that
// is used to signify if the key/value pair is deleted or not. Take a look at the NewDeletedEntry function.
// A little-endian system, stores the least-significant byte at the smallest address. What is special about 4 bytes key size or 4 bytes value size?
// The maximum integer stored by 4 bytes is 4,294,967,295 (2 ** 32 - 1), roughly ~4.2GB. This means each key or value size can not be greater than 4.2GB.
func (entry *Entry[Key]) encode() []byte {
	serializedKey := entry.key.Serialize()
	keySize, valueSize := uint32(len(serializedKey)), uint32(len(entry.value.value))+tombstoneMarkerSize

	encoded := make([]byte, reservedTimestampSize+reservedKeySize+reservedValueSize+keySize+valueSize)
	var offset uint32 = 0

	if entry.timestamp == 0 {
		littleEndian.PutUint32(encoded, uint32(int(entry.clock.Now())))
	} else {
		littleEndian.PutUint32(encoded, entry.timestamp)
	}
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

// decode performs the decode operation and returns an instance of StoredEntry
func decode(content []byte) *StoredEntry {
	var offset uint32 = 0
	storedEntry, _ := decodeFrom(content, offset)
	return storedEntry
}

// decodeMulti performs multiple decode operations and returns an array of MappedStoredEntry
// This method is invoked when a segment file needs to be read completely. This happens during reload and merge operations.
func decodeMulti[Key config.BitCaskKey](content []byte, keyMapper func([]byte) Key) []*MappedStoredEntry[Key] {
	contentLength := uint32(len(content))
	var offset uint32 = 0

	var entries []*MappedStoredEntry[Key]
	for offset < contentLength {
		entry, traversedOffset := decodeFrom(content, offset)
		entries = append(entries, &MappedStoredEntry[Key]{
			Key:         keyMapper(entry.Key),
			Value:       entry.Value,
			Deleted:     entry.Deleted,
			Timestamp:   entry.Timestamp,
			KeyOffset:   offset,
			EntryLength: traversedOffset,
		})
		offset = traversedOffset
	}
	return entries
}

// decodeFrom performs the decode operation.
// Encoding scheme consists of the following structure:
//
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ timestamp │ key_size │ value_size │ key │ value │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//
// In order to perform `decode`, the code reads the first 4 bytes to get the timestamp, next 4 bytes to get the key size, next 4 bytes to get the value size
// Note: the value size is the size including the length of the byte slice provided by the user and one byte for the tombstone marker
// Reading further from the offset to the offset+keySize return the actual key, followed by next read from offset to offset+valueSize which returns the actual value.
// DeletedFlag is determined by taking the last byte from the `value` byte slice and performing an AND operation with 0x01.
func decodeFrom(content []byte, offset uint32) (*StoredEntry, uint32) {
	timestamp := littleEndian.Uint32(content)
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
		Key:       serializedKey,
		Value:     value[:valueLength-1],
		Deleted:   value[valueLength-1]&0x01 == 0x01,
		Timestamp: timestamp,
	}, offset
}
