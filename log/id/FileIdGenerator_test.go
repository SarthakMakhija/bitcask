package id

import (
	"reflect"
	"sort"
	"sync"
	"testing"
)

func TestFileIdGenerator(t *testing.T) {
	generator := NewFileIdGenerator()
	if next := generator.Next(); next != 1 {
		t.Fatalf("Expected id to be 1 received %v", next)
	}
	if next := generator.Next(); next != 2 {
		t.Fatalf("Expected id to be 2 received %v", next)
	}
}

func TestFileIdGeneratorConcurrently(t *testing.T) {
	generator := NewFileIdGenerator()
	ids := make([]uint64, 2)
	var wg sync.WaitGroup

	wg.Add(1)
	go func() {
		defer wg.Done()
		ids[0] = generator.Next()
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		ids[1] = generator.Next()
	}()

	wg.Wait()
	sort.SliceStable(ids, func(i, j int) bool {
		return ids[i] < ids[j]
	})

	if !reflect.DeepEqual([]uint64{1, 2}, ids) {
		t.Fatalf("Expected file ids to be %v, received %v", []uint64{1, 2}, ids)
	}
}
