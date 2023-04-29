package merge

import (
	"bitcask/config"
	"bitcask/kv"
	"bitcask/log"
)

type Worker[Key config.BitCaskKey] struct {
	kvStore *kv.KVStore[Key]
	config  *config.MergeConfig[Key]
}

func NewWorker[Key config.BitCaskKey](kvStore *kv.KVStore[Key], config *config.MergeConfig[Key]) *Worker[Key] {
	return &Worker[Key]{
		kvStore: kvStore,
		config:  config,
	}
}

func (worker *Worker[Key]) begin() {
	var fileIds []uint64
	var segments [][]*log.MappedStoredEntry[Key]
	var err error

	if worker.config.ShouldReadAllSegments {
		fileIds, segments, err = worker.kvStore.ReadAllInactiveSegments(worker.config.KeyMapper)
	} else {
		fileIds, segments, err = worker.kvStore.ReadInactiveSegments(worker.config.TotalSegmentsToRead, worker.config.KeyMapper)
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
