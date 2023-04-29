package kv

import (
	"bitcask/kv/log"
	"testing"
)

func TestNewEntryWithFileId(t *testing.T) {
	entry := NewEntryFrom(&log.AppendEntryResponse{FileId: 10})
	if entry.FileId != 10 {
		t.Fatalf("Expected file id to be %v, received %v", 10, entry.FileId)
	}
}

func TestNewEntryWithOffset(t *testing.T) {
	entry := NewEntryFrom(&log.AppendEntryResponse{Offset: 32})
	if entry.Offset != 32 {
		t.Fatalf("Expected offset to be %v, received %v", 32, entry.Offset)
	}
}

func TestNewEntryWithEntryLength(t *testing.T) {
	entry := NewEntryFrom(&log.AppendEntryResponse{EntryLength: 64})
	if entry.EntryLength != 64 {
		t.Fatalf("Expected entry length to be %v, received %v", 63, entry.EntryLength)
	}
}
