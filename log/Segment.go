package log

type StoredEntry struct {
	Key   []byte
	Value []byte
}

type Segment[Key Serializable] struct {
	fileId   uint64
	filePath string
	store    *Store
}

func NewSegment[Key Serializable](fileId uint64, filePath string) (*Segment[Key], error) {
	store, err := NewStore(filePath)
	if err != nil {
		return nil, err
	}
	return &Segment[Key]{
		fileId:   fileId,
		filePath: filePath,
		store:    store,
	}, nil
}

func (segment *Segment[Key]) Append(entry *Entry[Key]) (int, error) {
	encoded := entry.encode()
	return len(encoded), segment.store.append(encoded)
}

func (segment *Segment[Key]) Read(position int64, size uint64) (*StoredEntry, error) {
	bytes, err := segment.store.read(position, size)
	if err != nil {
		return nil, err
	}
	key, value := decode(bytes)
	return &StoredEntry{Key: key, Value: value}, nil
}

func (segment *Segment[Key]) sync() {
	segment.store.sync()
}
