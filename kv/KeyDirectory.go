package kv

import (
	"bitcask/config"
	"bitcask/kv/log"
)

// KeyDirectory is the in-memory storage which maintains a mapping between keys and the position of those keys in the datafiles called segment.
// Entry maintains `FileId` identifying the file containing the key, `Offset` identifying the position in the file where the key is stored and
// the `EntryLength` identifying the length of the entry
type KeyDirectory[Key config.BitCaskKey] struct {
	entryByKey map[Key]*Entry
}

// NewKeyDirectory Creates a new instance of KeyDirectory
// The key in the hashmap is generically typed `BitCaskKey -> comparable + Serializable`. This choice results in an unsaid tradeoff. Let's understand this better.
// Assume `serializableKey` is used as the key in KeyDirectory. serializableKey is a type alias for string which implements Golang's comparable.
//
//	 type serializableKey string
//	 func (key serializableKey) Serialize() []byte {
//		  return []byte(key)
//	 }
//
// During merge and compaction process, we might merge K inactive segments and after those merged segments are written back to the disk, we
// will update the state of the merged keys in KeyDirectory. More on this in Worker.go.
// The representation of any Key on disk is a byte slice ([]byte), that means in order to update the state of the KeyDirectory,
// we need to convert a byte slice back to the `Key type` which is `serializableKey` in this example.
// That means deserialization of byte slice to Key type is needed during merge and compaction to update the state in the KeyDirectory.
// The cost of deserialization on this machine: "MacBook Pro (16-inch, 2019), 2.6 GHz 6-Core Intel Core i7, 16 GB 2667 MHz DDR4" was 27ns.
// In order to reduce this cost, we might be tempted to used byte slice as the key in the hashmap but Golang does not allow that.
// So, it might be worth comparing golang's HashMap to alternate data structures like `Skiplist` or `AVL tree` or a `Red black tree` and
// if the benchmarks for put and get in golang's HashMap are same as that of an alternative data structure,
// it makes sense to replace a generically typed HashMap with an alternative data structure that will store key as a byte slice.
func NewKeyDirectory[Key config.BitCaskKey](initialCapacity uint64) *KeyDirectory[Key] {
	return &KeyDirectory[Key]{
		entryByKey: make(map[Key]*Entry, initialCapacity),
	}
}

// Reload reloads the state of the KeyDirectory during start-up. As a part of reloading the state in bitcask model, all the inactive segments are read,
// and the keys from all the inactive segments are stored in the KeyDirectory.
// Riak's paper optimizes reloading by creating small sized hint files during merge and compaction.
// Hint files contain the keys and the metadata fields like fileId, fileOffset and entryLength, these hint files are referred during reload. This implementation does not create Hint file
func (keyDirectory *KeyDirectory[Key]) Reload(fileId uint64, entries []*log.MappedStoredEntry[Key]) {
	for _, entry := range entries {
		keyDirectory.entryByKey[entry.Key] = NewEntry(fileId, int64(entry.KeyOffset), entry.EntryLength)
	}
}

// Put puts a key and its entry as the value in the KeyDirectory
func (keyDirectory *KeyDirectory[Key]) Put(key Key, value *Entry) {
	keyDirectory.entryByKey[key] = value
}

// BulkUpdate performs bulk changes to the KeyDirectory state. This method is called during merge and compaction from KeyStore.
func (keyDirectory *KeyDirectory[Key]) BulkUpdate(changes []*log.WriteBackResponse[Key]) {
	for _, change := range changes {
		keyDirectory.entryByKey[change.Key] = NewEntryFrom(change.AppendEntryResponse)
	}
}

// Delete removes the key from the KeyDirectory
func (keyDirectory *KeyDirectory[Key]) Delete(key Key) {
	delete(keyDirectory.entryByKey, key)
}

// Get returns the Entry and a boolean to indicate if the value corresponding to the key is present in the KeyDirectory.
// Get returns nil, false if the value corresponding to the key is not present
// Get returns a pointer to an Entry, true if the value corresponding to the key is present
func (keyDirectory *KeyDirectory[Key]) Get(key Key) (*Entry, bool) {
	value, ok := keyDirectory.entryByKey[key]
	return value, ok
}
