package tests

import (
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
)

// Recovery Test
func TestRecoveryIsIdempotent(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	// First run
	eng1, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	_ = eng1.Put([]byte("a"), []byte("1"))
	_ = eng1.Put([]byte("b"), []byte("2"))
	_ = eng1.Delete([]byte("a"))

	_ = eng1.Close()

	// Second run (normal recovery)
	eng2, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}

	seqAfterFirstRecovery := eng2.Sequence()
	_ = eng2.Close()

	// Third run (duplicate recovery)
	eng3, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("reopen engine again: %v", err)
	}
	defer eng3.Close()

	// Sequence must NOT increase
	if eng3.Sequence() != seqAfterFirstRecovery {
		t.Fatalf(
			"expected seq=%d after duplicate recovery, got %d",
			seqAfterFirstRecovery,
			eng3.Sequence(),
		)
	}

	// Data must remain correct
	e, ok := eng3.MemtableGet([]byte("b"))
	if !ok || string(e.Value) != "2" {
		t.Fatalf("expected b=2 after duplicate recovery")
	}

	e, ok = eng3.MemtableGet([]byte("a"))
	if !ok || !e.Tombstone {
		t.Fatalf("expected tombstone for a after duplicate recovery")
	}
}
