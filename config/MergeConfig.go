package config

type MergeConfig[Key BitCaskKey] struct {
	TotalSegmentsToRead   int
	ShouldReadAllSegments bool
	KeyMapper             func([]byte) Key
}

func NewMergeConfig[Key BitCaskKey](totalSegmentsToRead int, keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		TotalSegmentsToRead:   totalSegmentsToRead,
		ShouldReadAllSegments: false,
		KeyMapper:             keyMapper,
	}
}

func NewMergeConfigWithAllSegmentsToRead[Key BitCaskKey](keyMapper func([]byte) Key) *MergeConfig[Key] {
	return &MergeConfig[Key]{
		ShouldReadAllSegments: true,
		KeyMapper:             keyMapper,
	}
}
