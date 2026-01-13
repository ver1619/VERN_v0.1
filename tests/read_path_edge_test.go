package tests

import (
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
)

// Read Path Edge Test
func TestReadActiveMemtableShadowsSSTable(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)
	cfg.MemtableSizeBytes = 1

	eng, _ := engine.Open(cfg)
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1")) // flush → SSTable
	_ = eng.Put([]byte("a"), []byte("2")) // active memtable

	val, ok, _ := eng.Get([]byte("a"))
	if !ok || string(val) != "2" {
		t.Fatalf("expected active memtable to shadow SSTable")
	}
}

func TestReadNewerSSTableShadowsOlder(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)
	cfg.MemtableSizeBytes = 1

	eng, _ := engine.Open(cfg)
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1")) // SSTable 1
	_ = eng.Put([]byte("b"), []byte("x")) // force new memtable
	_ = eng.Put([]byte("a"), []byte("2")) // SSTable 2

	val, ok, _ := eng.Get([]byte("a"))
	if !ok || string(val) != "2" {
		t.Fatalf("expected newer SSTable to shadow older")
	}
}

func TestReadTombstoneShadowsAll(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)
	cfg.MemtableSizeBytes = 1

	eng, _ := engine.Open(cfg)
	defer eng.Close()

	_ = eng.Put([]byte("a"), []byte("1")) // SSTable
	_ = eng.Delete([]byte("a"))           // tombstone in memtable

	_, ok, _ := eng.Get([]byte("a"))
	if ok {
		t.Fatalf("expected tombstone to suppress value")
	}
}

func TestReadMissingKey(t *testing.T) {
	dir := t.TempDir()
	cfg := config.DefaultConfig(dir)

	eng, _ := engine.Open(cfg)
	defer eng.Close()

	_, ok, err := eng.Get([]byte("missing"))
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Fatalf("expected missing key to return not found")
	}
}
