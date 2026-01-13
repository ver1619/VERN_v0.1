package engine

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"tecton_kv/config"
	"tecton_kv/memtable"
	"tecton_kv/sstable"
	"tecton_kv/wal"
)

// Engine is the top-level database object.
type Engine struct {
	cfg config.Config

	wal *wal.WAL

	active   *memtable.Memtable
	frozen   *memtable.Memtable
	sstables []string

	mu  sync.Mutex
	seq uint64
}

func Open(cfg config.Config) (*Engine, error) {
	_ = os.MkdirAll(cfg.DataDir, 0755)
	_ = os.MkdirAll(cfg.WALDir(), 0755)
	_ = os.MkdirAll(cfg.SSTableDir(), 0755)

	w, err := wal.Open(cfg.WALDir())
	if err != nil {
		return nil, err
	}

	active := memtable.New()
	var maxSeq uint64

	entries, err := w.Replay(0)
	if err != nil {
		return nil, err
	}

	for _, e := range entries {
		if e.Seq <= maxSeq {
			continue
		}
		if e.Tombstone {
			active.Delete(e.Key, e.Seq)
		} else {
			active.Put(e.Key, e.Value, e.Seq)
		}
		maxSeq = e.Seq
	}

	return &Engine{
		cfg:    cfg,
		wal:    w,
		active: active,
		frozen: nil,
		seq:    maxSeq,
	}, nil
}

func (e *Engine) Put(key, value []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.seq++
	if err := e.wal.AppendPut(e.seq, key, value); err != nil {
		e.seq--
		return err
	}

	e.active.Put(key, value, e.seq)
	e.maybeFlush()
	return nil
}

func (e *Engine) Delete(key []byte) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.seq++
	if err := e.wal.AppendDelete(e.seq, key); err != nil {
		e.seq--
		return err
	}

	e.active.Delete(key, e.seq)
	e.maybeFlush()
	return nil
}

func (e *Engine) maybeFlush() {
	if e.active.ApproximateSize() < e.cfg.MemtableSizeBytes {
		return
	}

	// Freeze
	e.frozen = e.active
	e.active = memtable.New()

	// Flush synchronously
	e.flushFrozen()
}

func (e *Engine) flushFrozen() {
	if e.frozen == nil {
		return
	}

	entries := e.frozen.AllEntriesSorted()
	if len(entries) == 0 {
		e.frozen = nil
		return
	}

	filename := fmt.Sprintf("sst_%d.sst", time.Now().UnixNano())
	tmpPath := filepath.Join(e.cfg.SSTableDir(), filename+".tmp")
	finalPath := filepath.Join(e.cfg.SSTableDir(), filename)

	if err := sstable.Write(tmpPath, entries); err != nil {
		panic(err) // v0.1: crash loudly
	}

	if err := os.Rename(tmpPath, finalPath); err != nil {
		panic(err)
	}

	e.sstables = append(e.sstables, finalPath)
	e.frozen = nil
}

// Sequence returns the current global sequence number.
// Intended for testing and diagnostics only.
func (e *Engine) Sequence() uint64 {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.seq
}

// MemtableGet returns the latest entry for a key.
// Intended for testing and diagnostics only.
func (e *Engine) MemtableGet(key []byte) (memtable.Entry, bool) {
	return e.active.Get(key)
}

// Close shuts down the engine.
func (e *Engine) Close() error {
	return e.wal.Close()
}

// Get returns the latest value for a key.
// If the key is deleted or not found, found = false(not found) is returned.
func (e *Engine) Get(key []byte) ([]byte, bool, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	var (
		bestSeq uint64
		found   bool
		value   []byte
	)

	// 1. Active Memtable
	if e.active != nil {
		if entry, ok := e.active.Get(key); ok {
			if entry.Seq > bestSeq {
				bestSeq = entry.Seq
				found = !entry.Tombstone
				value = entry.Value
			}
		}
	}

	// 2. Frozen Memtable
	if e.frozen != nil {
		if entry, ok := e.frozen.Get(key); ok {
			if entry.Seq > bestSeq {
				bestSeq = entry.Seq
				found = !entry.Tombstone
				value = entry.Value
			}
		}
	}

	// 3. SSTables (newest → oldest)
	for i := len(e.sstables) - 1; i >= 0; i-- {
		st, err := sstable.Open(e.sstables[i])
		if err != nil {
			return nil, false, err
		}

		entry, ok, err := st.Get(key)
		st.Close()
		if err != nil {
			return nil, false, err
		}

		if ok && entry.Seq > bestSeq {
			bestSeq = entry.Seq
			found = !entry.Tombstone
			value = entry.Value
		}

		// Optimization: if we already found a newer entry,
		// older SSTables cannot override it.
		if found || bestSeq > 0 {
			break
		}
	}

	if !found {
		return nil, false, nil
	}

	return value, true, nil
}
