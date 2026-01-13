package config

import "path/filepath"

// Config holds all tunable parameters for TectonKV.
type Config struct {
	// Root directory where all data is stored
	DataDir string

	// Maximum size of the Memtable in bytes before flush
	MemtableSizeBytes int64
}

// DefaultConfig returns a safe default configuration.
func DefaultConfig(dataDir string) Config {
	return Config{
		DataDir:           dataDir,
		MemtableSizeBytes: 2 * 1024 * 1024, // 2MB (default)
	}
}

// WALDir returns the directory for WAL files.
func (c Config) WALDir() string {
	return filepath.Join(c.DataDir, "wal")
}

// SSTableDir returns the directory for SSTables.
func (c Config) SSTableDir() string {
	return filepath.Join(c.DataDir, "sstables")
}
