package config

import "time"

type MergeConfig[Key BitCaskKey] struct {
	totalSegmentsToRead   int
	shouldReadAllSegments bool
	keyMapper             func([]byte) Key
	runMergeEvery         time.Duration
}

func NewMergeConfig[Key BitCaskKey](totalSegmentsToRead int, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		totalSegmentsToRead:   totalSegmentsToRead,
		shouldReadAllSegments: false,
		keyMapper:             keyMapper,
		runMergeEvery:         5 * time.Minute,
	}
}

func NewMergeConfigWithDuration[Key BitCaskKey](totalSegmentsToRead int, runMergeEvery time.Duration, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		totalSegmentsToRead:   totalSegmentsToRead,
		shouldReadAllSegments: false,
		keyMapper:             keyMapper,
		runMergeEvery:         runMergeEvery,
	}
}

func NewMergeConfigWithAllSegmentsToRead[Key BitCaskKey](keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		shouldReadAllSegments: true,
		keyMapper:             keyMapper,
		runMergeEvery:         5 * time.Minute,
	}
}

func NewMergeConfigWithAllSegmentsToReadEveryFixedDuration[Key BitCaskKey](runMergeEvery time.Duration, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		shouldReadAllSegments: true,
		keyMapper:             keyMapper,
		runMergeEvery:         runMergeEvery,
	}
}

func (m *MergeConfig[Key]) TotalSegmentsToRead() int {
	return m.totalSegmentsToRead
}

func (m *MergeConfig[Key]) ShouldReadAllSegments() bool {
	return m.shouldReadAllSegments
}

func (m *MergeConfig[Key]) KeyMapper() func([]byte) Key {
	return m.keyMapper
}

func (m *MergeConfig[Key]) RunMergeEvery() time.Duration {
	return m.runMergeEvery
}
