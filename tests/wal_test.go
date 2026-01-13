package tests

import (
	"testing"

	"tecton_kv/wal"
)

// WAL Test
func TestWALAppendAndReplay(t *testing.T) {
	dir := t.TempDir()

	w, err := wal.Open(dir)
	if err != nil {
		t.Fatalf("open wal: %v", err)
	}
	defer w.Close()

	if err := w.AppendPut(1, []byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendPut(2, []byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}
	if err := w.AppendDelete(3, []byte("a")); err != nil {
		t.Fatal(err)
	}

	entries, err := w.Replay(0)
	if err != nil {
		t.Fatal(err)
	}

	if len(entries) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(entries))
	}

	if !entries[2].Tombstone {
		t.Fatalf("expected tombstone for key a")
	}
}
