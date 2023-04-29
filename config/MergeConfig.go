package config

import "time"

type MergeConfig[Key BitCaskKey] struct {
	TotalSegmentsToRead   int
	ShouldReadAllSegments bool
	KeyMapper             func([]byte) Key
	RunMergeEvery         time.Duration
}

func NewMergeConfig[Key BitCaskKey](totalSegmentsToRead int, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		TotalSegmentsToRead:   totalSegmentsToRead,
		ShouldReadAllSegments: false,
		KeyMapper:             keyMapper,
		RunMergeEvery:         5 * time.Minute,
	}
}

func NewMergeConfigWithDuration[Key BitCaskKey](totalSegmentsToRead int, runMergeEvery time.Duration, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		TotalSegmentsToRead:   totalSegmentsToRead,
		ShouldReadAllSegments: false,
		KeyMapper:             keyMapper,
		RunMergeEvery:         runMergeEvery,
	}
}

func NewMergeConfigWithAllSegmentsToRead[Key BitCaskKey](keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		ShouldReadAllSegments: true,
		KeyMapper:             keyMapper,
		RunMergeEvery:         5 * time.Minute,
	}
}

func NewMergeConfigWithAllSegmentsToReadEveryFixedDuration[Key BitCaskKey](runMergeEvery time.Duration, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		ShouldReadAllSegments: true,
		KeyMapper:             keyMapper,
		RunMergeEvery:         runMergeEvery,
	}
}
