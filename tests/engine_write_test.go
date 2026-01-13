package tests

import (
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
)

// Write and Delete Test

func TestEnginePutAndDelete(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("open engine: %v", err)
	}
	defer eng.Close()

	if err := eng.Put([]byte("a"), []byte("1")); err != nil {
		t.Fatal(err)
	}
	if err := eng.Delete([]byte("a")); err != nil {
		t.Fatal(err)
	}
}

func TestEngineSequenceIncrements(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	eng, _ := engine.Open(cfg)
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1"))
	_ = eng.Put([]byte("b"), []byte("2"))
	_ = eng.Delete([]byte("a"))

	if eng.Sequence() != 3 {
		t.Fatalf("expected seq=3, got %d", eng.Sequence())
	}
}
