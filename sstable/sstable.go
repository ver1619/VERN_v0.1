package sstable

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

const (
	magicNumber   = 0x544B5631 // "TKV1"
	flagTombstone = 0x01
)

// Entry is a persisted key entry.
type Entry struct {
	Key       []byte
	Value     []byte
	Seq       uint64
	Tombstone bool
}

// SSTable represents an opened SSTable file.
type SSTable struct {
	file   *os.File
	index  map[string]int64
	maxSeq uint64
}

// Write creates a new SSTable at path.
func Write(path string, entries []Entry) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	index := make(map[string]int64)

	var offset int64
	for _, e := range entries {
		index[string(e.Key)] = offset

		var flags byte
		if e.Tombstone {
			flags = flagTombstone
		}

		keyLen := uint32(len(e.Key))
		valLen := uint32(len(e.Value))

		if err := binary.Write(f, binary.BigEndian, keyLen); err != nil {
			return err
		}
		if err := binary.Write(f, binary.BigEndian, valLen); err != nil {
			return err
		}
		if err := binary.Write(f, binary.BigEndian, e.Seq); err != nil {
			return err
		}
		if _, err := f.Write([]byte{flags}); err != nil {
			return err
		}
		if _, err := f.Write(e.Key); err != nil {
			return err
		}
		if _, err := f.Write(e.Value); err != nil {
			return err
		}

		offset += int64(4 + 4 + 8 + 1 + len(e.Key) + len(e.Value))
	}

	indexOffset := offset

	// Write index block
	for k, off := range index {
		key := []byte(k)
		if err := binary.Write(f, binary.BigEndian, uint32(len(key))); err != nil {
			return err
		}
		if _, err := f.Write(key); err != nil {
			return err
		}
		if err := binary.Write(f, binary.BigEndian, off); err != nil {
			return err
		}
		offset += int64(4 + len(key) + 8)
	}

	// Write footer
	if err := binary.Write(f, binary.BigEndian, indexOffset); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, uint64(len(entries))); err != nil {
		return err
	}

	var maxSeq uint64
	for _, e := range entries {
		if e.Seq > maxSeq {
			maxSeq = e.Seq
		}
	}
	if err := binary.Write(f, binary.BigEndian, maxSeq); err != nil {
		return err
	}
	if err := binary.Write(f, binary.BigEndian, uint32(magicNumber)); err != nil {
		return err
	}

	return f.Sync()
}

// Open opens an SSTable for read.
func Open(path string) (*SSTable, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}

	stat, _ := f.Stat()
	size := stat.Size()

	// Read footer
	if _, err := f.Seek(size-28, io.SeekStart); err != nil {
		return nil, err
	}

	var indexOffset uint64
	var entryCount uint64
	var maxSeq uint64
	var magic uint32

	if err := binary.Read(f, binary.BigEndian, &indexOffset); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.BigEndian, &entryCount); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.BigEndian, &maxSeq); err != nil {
		return nil, err
	}
	if err := binary.Read(f, binary.BigEndian, &magic); err != nil {
		return nil, err
	}

	if magic != magicNumber {
		return nil, fmt.Errorf("invalid sstable magic")
	}

	index := make(map[string]int64)

	// Read index block
	if _, err := f.Seek(int64(indexOffset), io.SeekStart); err != nil {
		return nil, err
	}

	for i := uint64(0); i < entryCount; i++ {
		var keyLen uint32
		if err := binary.Read(f, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(f, key); err != nil {
			return nil, err
		}

		var off int64
		if err := binary.Read(f, binary.BigEndian, &off); err != nil {
			return nil, err
		}

		index[string(key)] = off
	}

	return &SSTable{
		file:   f,
		index:  index,
		maxSeq: maxSeq,
	}, nil
}

// Get returns an entry for a key.
func (s *SSTable) Get(key []byte) (Entry, bool, error) {
	off, ok := s.index[string(key)]
	if !ok {
		return Entry{}, false, nil
	}

	if _, err := s.file.Seek(off, io.SeekStart); err != nil {
		return Entry{}, false, err
	}

	var keyLen uint32
	var valLen uint32
	var seq uint64

	if err := binary.Read(s.file, binary.BigEndian, &keyLen); err != nil {
		return Entry{}, false, err
	}
	if err := binary.Read(s.file, binary.BigEndian, &valLen); err != nil {
		return Entry{}, false, err
	}
	if err := binary.Read(s.file, binary.BigEndian, &seq); err != nil {
		return Entry{}, false, err
	}

	flags := make([]byte, 1)
	if _, err := io.ReadFull(s.file, flags); err != nil {
		return Entry{}, false, err
	}

	k := make([]byte, keyLen)
	if _, err := io.ReadFull(s.file, k); err != nil {
		return Entry{}, false, err
	}

	v := make([]byte, valLen)
	if _, err := io.ReadFull(s.file, v); err != nil {
		return Entry{}, false, err
	}

	return Entry{
		Key:       k,
		Value:     v,
		Seq:       seq,
		Tombstone: flags[0] == flagTombstone,
	}, true, nil
}

// Close closes the SSTable.
func (s *SSTable) Close() error {
	return s.file.Close()
}
