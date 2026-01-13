package tests

import (
	"testing"

	"tecton_kv/memtable"
)

// Memtable Test
func TestMemtablePutAndGet(t *testing.T) {
	mt := memtable.New()

	mt.Put([]byte("a"), []byte("1"), 1)
	mt.Put([]byte("b"), []byte("2"), 2)

	e, ok := mt.Get([]byte("a"))
	if !ok || string(e.Value) != "1" {
		t.Fatalf("expected a=1")
	}
}

func TestMemtableOverwriteBySeq(t *testing.T) {
	mt := memtable.New()

	mt.Put([]byte("a"), []byte("1"), 1)
	mt.Put([]byte("a"), []byte("2"), 2)

	e, _ := mt.Get([]byte("a"))
	if string(e.Value) != "2" {
		t.Fatalf("expected newest value")
	}
}

func TestMemtableTombstone(t *testing.T) {
	mt := memtable.New()

	mt.Put([]byte("a"), []byte("1"), 1)
	mt.Delete([]byte("a"), 2)

	e, ok := mt.Get([]byte("a"))
	if !ok || !e.Tombstone {
		t.Fatalf("expected tombstone")
	}
}
