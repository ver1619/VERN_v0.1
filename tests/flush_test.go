package tests

import (
	"os"
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
)

// Memtable Flush Test
func TestMemtableFlushCreatesSSTable(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)
	cfg.MemtableSizeBytes = 32 // force flush quickly

	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatal(err)
	}
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"))
	_ = eng.Put([]byte("b"), []byte("2"))
	_ = eng.Put([]byte("c"), []byte("3"))

	files, _ := os.ReadDir(cfg.SSTableDir())
	if len(files) == 0 {
		t.Fatalf("expected at least one SSTable")
	}
}
