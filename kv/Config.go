package kv

import "bitcask/clock"

type Config struct {
	directory            string
	maxSegmentSizeBytes  uint64
	keyDirectoryCapacity uint64
	clock                clock.Clock
}

func NewConfig(directory string, maxSegmentSizeBytes uint64, keyDirectoryCapacity uint64) *Config {
	return NewConfigWithClock(directory, maxSegmentSizeBytes, keyDirectoryCapacity, clock.NewSystemClock())
}

func NewConfigWithClock(directory string, maxSegmentSizeBytes uint64, keyDirectoryCapacity uint64, clock clock.Clock) *Config {
	return &Config{
		directory:            directory,
		maxSegmentSizeBytes:  maxSegmentSizeBytes,
		keyDirectoryCapacity: keyDirectoryCapacity,
		clock:                clock,
	}
}

func (config *Config) Directory() string {
	return config.directory
}

func (config *Config) MaxSegmentSizeInBytes() uint64 {
	return config.maxSegmentSizeBytes
}

func (config *Config) KeyDirectoryCapacity() uint64 {
	return config.keyDirectoryCapacity
}

func (config *Config) Clock() clock.Clock {
	return config.clock
}
