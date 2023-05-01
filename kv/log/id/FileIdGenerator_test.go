package id

import (
	"testing"
)

type FixedClock struct{}

func (clock *FixedClock) Now() int64 {
	return 100
}

func TestFileIdGenerator(t *testing.T) {
	generator := NewTimestampBasedFileIdGenerator(&FixedClock{})
	if next := generator.Next(); next != 100 {
		t.Fatalf("Expected id to be 1 received %v", next)
	}
}
