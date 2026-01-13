package memtable

import (
	"bytes"
	"math/rand"
	"tecton_kv/sstable"
	"time"
)

const (
	maxLevel    = 12
	probability = 0.5
)

// Entry represents a single key-version.
type Entry struct {
	Key       []byte
	Value     []byte
	Seq       uint64
	Tombstone bool
}

// node is a SkipList node.
type node struct {
	entry   Entry
	forward []*node
}

// Memtable is a SkipList-backed in-memory table.
type Memtable struct {
	head  *node
	level int
	size  int64
}

// New creates an empty Memtable.
func New() *Memtable {
	rand.Seed(time.Now().UnixNano())

	head := &node{
		forward: make([]*node, maxLevel),
	}

	return &Memtable{
		head:  head,
		level: 1,
	}
}

// approximateSize returns current size in bytes.
func (m *Memtable) approximateSize() int64 {
	return m.size
}

// randomLevel generates a random level.
func randomLevel() int {
	lvl := 1
	for rand.Float64() < probability && lvl < maxLevel {
		lvl++
	}
	return lvl
}

// Put inserts key.
func (m *Memtable) Put(key, value []byte, seq uint64) {
	m.insert(Entry{
		Key:       key,
		Value:     value,
		Seq:       seq,
		Tombstone: false,
	})
}

// Delete inserts a tombstone.
func (m *Memtable) Delete(key []byte, seq uint64) {
	m.insert(Entry{
		Key:       key,
		Seq:       seq,
		Tombstone: true,
	})
}

func (m *Memtable) insert(e Entry) {
	update := make([]*node, maxLevel)
	x := m.head

	// find positions
	for i := m.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			bytes.Compare(x.forward[i].entry.Key, e.Key) < 0 {
			x = x.forward[i]
		}
		update[i] = x
	}

	// check existing key
	x = x.forward[0]
	if x != nil && bytes.Equal(x.entry.Key, e.Key) {
		if e.Seq > x.entry.Seq {
			m.size -= int64(len(x.entry.Key) + len(x.entry.Value))
			x.entry = e
			m.size += int64(len(e.Key) + len(e.Value))
		}
		return
	}

	lvl := randomLevel()
	if lvl > m.level {
		for i := m.level; i < lvl; i++ {
			update[i] = m.head
		}
		m.level = lvl
	}

	n := &node{
		entry:   e,
		forward: make([]*node, lvl),
	}

	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}

	m.size += int64(len(e.Key) + len(e.Value))
}

// Get returns the newest entry for a key.
func (m *Memtable) Get(key []byte) (Entry, bool) {
	x := m.head

	for i := m.level - 1; i >= 0; i-- {
		for x.forward[i] != nil &&
			bytes.Compare(x.forward[i].entry.Key, key) < 0 {
			x = x.forward[i]
		}
	}

	x = x.forward[0]
	if x != nil && bytes.Equal(x.entry.Key, key) {
		return x.entry, true
	}

	return Entry{}, false
}

// ApproximateSize returns approximate memory usage.
func (m *Memtable) ApproximateSize() int64 {
	return m.approximateSize()
}

// AllEntriesSorted returns all entries sorted by key.
func (m *Memtable) AllEntriesSorted() []sstable.Entry {
	var entries []sstable.Entry
	x := m.head.forward[0]

	for x != nil {
		e := x.entry
		entries = append(entries, sstable.Entry{
			Key:       e.Key,
			Value:     e.Value,
			Seq:       e.Seq,
			Tombstone: e.Tombstone,
		})
		x = x.forward[0]
	}

	return entries
}
