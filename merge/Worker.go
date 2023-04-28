package merge

import (
	"bitcask/config"
	"bitcask/kv"
)

type Worker[Key config.BitCaskKey] struct {
	kvStore   *kv.KVStore[Key]
	keyMapper func([]byte) Key
}

func NewWorker[Key config.BitCaskKey](kvStore *kv.KVStore[Key], keyMapper func([]byte) Key) *Worker[Key] {
	return &Worker[Key]{
		kvStore:   kvStore,
		keyMapper: keyMapper,
	}
}

func (worker *Worker[Key]) begin() {
	fileIds, pair, err := worker.kvStore.ReadPairOfInactiveSegment(worker.keyMapper)
	if err == nil {
		mergedState := NewMergedState[Key]()
		mergedState.merge(pair[0], pair[0])
		_ = worker.kvStore.WriteBack(fileIds, mergedState.valueByKey)
	}
}
