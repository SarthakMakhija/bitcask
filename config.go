package bitcask

type Config struct {
	directory            string
	maxSegmentSizeBytes  uint64
	keyDirectoryCapacity uint64
}

func NewConfig(directory string, maxSegmentSizeBytes uint64, keyDirectoryCapacity uint64) *Config {
	return &Config{
		directory:            directory,
		maxSegmentSizeBytes:  maxSegmentSizeBytes,
		keyDirectoryCapacity: keyDirectoryCapacity,
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
