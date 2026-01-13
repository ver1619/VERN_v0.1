package tests

import (
	"os"
	"path/filepath"
	"testing"

	"tecton_kv/config"
	"tecton_kv/engine"
)

// Engine Test
func TestPhase0EngineOpenCreatesDirectories(t *testing.T) {
	dir := t.TempDir()

	cfg := config.DefaultConfig(dir)

	eng, err := engine.Open(cfg)
	if err != nil {
		t.Fatalf("failed to open engine: %v", err)
	}
	defer eng.Close()

	paths := []string{
		cfg.DataDir,
		cfg.WALDir(),
		cfg.SSTableDir(),
	}

	for _, p := range paths {
		if _, err := os.Stat(p); err != nil {
			t.Fatalf("expected path to exist: %s", filepath.Base(p))
		}
	}
}
