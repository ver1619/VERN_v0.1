package tests

import (
	"os"
	"testing"

	"tecton_kv/sstable"
)

// SSTable Test
func TestSSTableWriteAndGet(t *testing.T) {
	dir := t.TempDir()
	path := dir + "/test.sst"

	entries := []sstable.Entry{
		{Key: []byte("a"), Value: []byte("1"), Seq: 1},
		{Key: []byte("b"), Value: []byte("2"), Seq: 2},
		{Key: []byte("c"), Seq: 3, Tombstone: true},
	}

	if err := sstable.Write(path, entries); err != nil {
		t.Fatalf("write sstable: %v", err)
	}

	st, err := sstable.Open(path)
	if err != nil {
		t.Fatalf("open sstable: %v", err)
	}
	defer st.Close()

	e, ok, err := st.Get([]byte("b"))
	if err != nil || !ok || string(e.Value) != "2" {
		t.Fatalf("expected b=2")
	}

	e, ok, _ = st.Get([]byte("c"))
	if !ok || !e.Tombstone {
		t.Fatalf("expected tombstone for c")
	}

	_, ok, _ = st.Get([]byte("x"))
	if ok {
		t.Fatalf("did not expect key x")
	}

	_ = os.Remove(path)
}
