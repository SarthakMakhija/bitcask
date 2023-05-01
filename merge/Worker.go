package merge

import (
	"bitcask/config"
	"bitcask/kv"
	"bitcask/kv/log"
	"time"
)

// Worker encapsulates KVStore and MergeConfig. Worker is an abstraction inside merge package that performs merge of inactive segment files every fixed duration
type Worker[Key config.BitCaskKey] struct {
	kvStore *kv.KVStore[Key]
	config  *config.MergeConfig[Key]
	quit    chan struct{}
}

// NewWorker creates an instance of Worker and starts the Worker
func NewWorker[Key config.BitCaskKey](kvStore *kv.KVStore[Key], config *config.MergeConfig[Key]) *Worker[Key] {
	worker := &Worker[Key]{
		kvStore: kvStore,
		config:  config,
		quit:    make(chan struct{}),
	}
	worker.start()
	return worker
}

// start is invoked from the NewWorker function. It spins a goroutine that runs every fixed duration defined in `runMergeEvery` field of MergeConfig
func (worker *Worker[Key]) start() {
	ticker := time.NewTicker(worker.config.RunMergeEvery())
	go func() {
		for {
			select {
			case <-ticker.C:
				worker.beginMerge()
			case <-worker.quit:
				ticker.Stop()
				return
			}
		}
	}()
}

// beginMerge performs the merge operation. It is invoked every `runMergeEvery` duration defined in the MergeConfig
// As a part of merge process, either all the inactive segments files are read or any of the K inactive segment files are read in memory.
// Once those files are loaded in memory, an instance of MergedState is created that maintains a HashMap of Key and MappedStoredEntry.
// MergedState is responsible for performing the merge operation. Merge operation is all about picking the latest value of a key
// if it is present in 2 or more segment files.
// Once the merge operation is done, the changes are written back to new inactive files and the in-memory state is updated in KeyDirectory.

// Why do we need to update the in-memory state?
// Assume a Key K1 with Value V1 and Timestamp T1 is present in the segment file F1. This key gets updated with value V2 at a later timestamp T2
// and these changes were written to a new active segment file F2. At some point in time, F2 becomes inactive.
// At this stage the KeyDirectory will contain the following mapping for the key K1, <K1 => {F2, Offset, EntryLength}>.
//  Segment file F1
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ T1        │ key_size │ value_size │ K1  │ V1    │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//  Segment file F2
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ T2        │ key_size │ value_size │ K1  │ V2    │
//	└───────────┴──────────┴────────────┴─────┴───────┘

// KeyDirectory contains K1 pointing to the offset of K1 in the segment file F2.
// With this background, let's consider that the merge process starts, and it reads the contents of F1 and F2 and performs a merge.
// The merge writes the key K1 with its new value V2 and timestamp T2 in a new file F3, and deletes files F1 and F2.

//	 Segment file F3
//	┌───────────┬──────────┬────────────┬─────┬───────┐
//	│ T2        │ key_size │ value_size │ K1  │ V2    │
//	└───────────┴──────────┴────────────┴─────┴───────┘
//
// The moment merge process is done, the state of Key K1 needs to be updated in the KeyDirectory to point to the new offset in the new file.
func (worker *Worker[Key]) beginMerge() {
	var fileIds []uint64
	var segments [][]*log.MappedStoredEntry[Key]
	var err error

	if worker.config.ShouldReadAllSegments() {
		fileIds, segments, err = worker.kvStore.ReadAllInactiveSegments(worker.config.KeyMapper())
	} else {
		fileIds, segments, err = worker.kvStore.ReadInactiveSegments(worker.config.TotalSegmentsToRead(), worker.config.KeyMapper())
	}

	if err == nil && len(segments) >= 2 {
		mergedState := NewMergedState[Key]()
		mergedState.takeAll(segments[0])

		for index := 1; index < len(segments); index++ {
			mergedState.mergeWith(segments[index])
		}
		_ = worker.kvStore.WriteBack(fileIds, mergedState.valueByKey)
	}
}

// Stop closes the quit channel which is used to signal the merge goroutine to stop
func (worker *Worker[Key]) Stop() {
	close(worker.quit)
}
