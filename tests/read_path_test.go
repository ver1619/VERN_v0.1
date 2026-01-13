package tests

import (
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
)

// Read Path Test
func TestReadFromActiveMemtable(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1"))

	val, ok, err := eng.Get([]byte("a"))
	if err != nil || !ok || string(val) != "1" {
		t.Fatalf("expected a=1")
	}
}

func TestReadAfterFlush(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)
	cfg.MemtableSizeBytes = 1

	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1")) // triggers flush

	val, ok, err := eng.Get([]byte("a"))
	if err != nil || !ok || string(val) != "1" {
		t.Fatalf("expected a=1 from SSTable")
	}
}

func TestReadWithTombstone(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1"))
	_ = eng.Delete([]byte("a"))

	_, ok, err := eng.Get([]byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected a to be deleted")
	}
}
