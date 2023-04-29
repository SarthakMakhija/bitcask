package config

import "bitcask/clock"

type Config[Key BitCaskKey] struct {
	directory            string
	maxSegmentSizeBytes  uint64
	keyDirectoryCapacity uint64
	mergeConfig          *MergeConfig[Key]
	clock                clock.Clock
}

func NewConfig[Key BitCaskKey](directory string, maxSegmentSizeBytes uint64, keyDirectoryCapacity uint64, mergeConfig *MergeConfig[Key]) *Config[Key] {
	return NewConfigWithClock[Key](directory, maxSegmentSizeBytes, keyDirectoryCapacity, mergeConfig, clock.NewSystemClock())
}

func NewConfigWithClock[Key BitCaskKey](
	directory string,
	maxSegmentSizeBytes uint64,
	keyDirectoryCapacity uint64,
	mergeConfig *MergeConfig[Key],
	clock clock.Clock) *Config[Key] {

	return &Config[Key]{
		directory:            directory,
		maxSegmentSizeBytes:  maxSegmentSizeBytes,
		keyDirectoryCapacity: keyDirectoryCapacity,
		mergeConfig:          mergeConfig,
		clock:                clock,
	}
}

func (config *Config[Key]) Directory() string {
	return config.directory
}

func (config *Config[Key]) MaxSegmentSizeInBytes() uint64 {
	return config.maxSegmentSizeBytes
}

func (config *Config[Key]) KeyDirectoryCapacity() uint64 {
	return config.keyDirectoryCapacity
}

func (config *Config[Key]) Clock() clock.Clock {
	return config.clock
}

func (config *Config[Key]) MergeConfig() *MergeConfig[Key] {
	return config.mergeConfig
}
