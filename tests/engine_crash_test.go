package tests

import (
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
	"tecton_kv/memtable"
)

func TestEngineCrashRecoveryFromWAL(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	// ---- First run ----
	eng1, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}

	if err := eng1.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := eng1.Put([]byte("b"), []byte("2")); err != nil {
		t.Fatal(err)
	}

	// Simulate crash: close without reading state
	if err := eng1.Close(); err != nil {
		t.Fatal(err)
	}

	// --- Second run (after crash) ---
	eng2, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("reopen engine: %v", err)
	}
	defer eng2.Close()

	// Recovered state must include both writes
	assertMemtableValue(t, eng2, "a", "1")
	assertMemtableValue(t, eng2, "b", "2")
}

func engMemtableGet(e *engine.Engine, key []byte) (memtable.Entry, bool) {
	return e.MemtableGet(key)
}

func assertMemtableValue(t *testing.T, eng *engine.Engine, key, expected string) {
	t.Helper()

	e, ok := engMemtableGet(eng, []byte(key))
	if !ok {
		t.Fatalf("expected key %s to exist", key)
	}
	if string(e.Value) != expected {
		t.Fatalf("expected %s=%s, got %s", key, expected, string(e.Value))
	}
}

func TestEngineCrashRecoveryWithDelete(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	eng1, _ := engine.Open(cfg)

	_ = eng1.Put([]byte("a"), []byte("1"))
	_ = eng1.Delete([]byte("a"))
	_ = eng1.Close()

	eng2, _ := engine.Open(cfg)
	defer eng2.Close()

	e, ok := eng2.MemtableGet([]byte("a"))
	if !ok || !e.Tombstone {
		t.Fatalf("expected tombstone for key a after restart")
	}
}
