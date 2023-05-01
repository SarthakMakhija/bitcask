package merge

import (
	"bitcask/config"
	"bitcask/kv/log"
)

// MergedState encapsulates key and its entry from inactive segment files
type MergedState[Key config.BitCaskKey] struct {
	valueByKey  map[Key]*log.MappedStoredEntry[Key]
	deletedKeys map[Key]*log.MappedStoredEntry[Key]
}

// NewMergedState creates a new instance of MergedState
func NewMergedState[Key config.BitCaskKey]() *MergedState[Key] {
	return &MergedState[Key]{
		valueByKey:  make(map[Key]*log.MappedStoredEntry[Key]),
		deletedKeys: make(map[Key]*log.MappedStoredEntry[Key]),
	}
}

// merge performs a merge operation between 2 sets of entries
func (mergedState *MergedState[Key]) merge(entries []*log.MappedStoredEntry[Key], otherEntries []*log.MappedStoredEntry[Key]) {
	mergedState.takeAll(entries)
	mergedState.mergeWith(otherEntries)
}

// takeAll accepts all the entries as is and dumps these entries in the hashmap
func (mergedState *MergedState[Key]) takeAll(mappedEntries []*log.MappedStoredEntry[Key]) {
	for _, entry := range mappedEntries {
		if entry.Deleted {
			mergedState.deletedKeys[entry.Key] = entry
		} else {
			mergedState.valueByKey[entry.Key] = entry
		}
	}
}

// mergeWith performs a merge operation with the new set of entries based on timestamp. The value of key with the latest timestamp is retained
// Tests server as a better documentation for this method
func (mergedState *MergedState[Key]) mergeWith(mappedEntries []*log.MappedStoredEntry[Key]) {
	for _, newEntry := range mappedEntries {
		existing, ok := mergedState.valueByKey[newEntry.Key]
		if !ok {
			deletedEntry, ok := mergedState.deletedKeys[newEntry.Key]
			if !ok {
				mergedState.valueByKey[newEntry.Key] = newEntry
			} else {
				mergedState.maybeUpdate(deletedEntry, newEntry)
				delete(mergedState.deletedKeys, newEntry.Key)
			}
		} else {
			mergedState.maybeUpdate(existing, newEntry)
		}
	}
}

func (mergedState *MergedState[Key]) maybeUpdate(existingEntry *log.MappedStoredEntry[Key], newEntry *log.MappedStoredEntry[Key]) {
	if newEntry.Timestamp > existingEntry.Timestamp {
		if newEntry.Deleted {
			delete(mergedState.valueByKey, existingEntry.Key)
		} else {
			mergedState.valueByKey[existingEntry.Key] = newEntry
		}
	}
}
