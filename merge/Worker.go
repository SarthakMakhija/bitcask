package merge

import (
	"bitcask/config"
	"bitcask/kv"
	"bitcask/kv/log"
	"time"
)

type Worker[Key config.BitCaskKey] struct {
	kvStore *kv.KVStore[Key]
	config  *config.MergeConfig[Key]
	quit    chan struct{}
}

func NewWorker[Key config.BitCaskKey](kvStore *kv.KVStore[Key], config *config.MergeConfig[Key]) *Worker[Key] {
	return &Worker[Key]{
		kvStore: kvStore,
		config:  config,
		quit:    make(chan struct{}),
	}
}

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

func (worker *Worker[Key]) stop() {
	close(worker.quit)
}
