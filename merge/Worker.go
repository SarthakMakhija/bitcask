package merge

import (
	"bitcask/config"
	"bitcask/kv"
)

type Worker[Key config.BitCaskKey] struct {
	kvStore            *kv.KVStore[Key]
	keyMapper          func([]byte) Key
	totalSegmentToRead uint8
}

func NewWorker[Key config.BitCaskKey](kvStore *kv.KVStore[Key], totalSegmentToRead uint8, keyMapper func([]byte) Key) *Worker[Key] {
	return &Worker[Key]{
		kvStore:            kvStore,
		keyMapper:          keyMapper,
		totalSegmentToRead: totalSegmentToRead,
	}
}

func (worker *Worker[Key]) begin() {
	fileIds, segments, err := worker.kvStore.ReadInactiveSegments(worker.totalSegmentToRead, worker.keyMapper)
	if err == nil && len(segments) >= 2 {
		mergedState := NewMergedState[Key]()
		mergedState.takeAll(segments[0])

		for index := 1; index < len(segments); index++ {
			mergedState.mergeWith(segments[index])
		}
		_ = worker.kvStore.WriteBack(fileIds, mergedState.valueByKey)
	}
}
