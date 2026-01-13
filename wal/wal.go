package wal

import (
	"encoding/binary"
	"io"
	"os"
	"path/filepath"
)

const (
	recordPut    byte = 1
	recordDelete byte = 2
)

// WAL (Write-Ahead Log)
type WAL struct {
	file *os.File
}

// Open opens (or creates) a WAL file in append mode.
func Open(dir string) (*WAL, error) {
	path := filepath.Join(dir, "wal.log")

	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	return &WAL{file: f}, nil
}

// AppendPut appends a PUT record to the WAL.
func (w *WAL) AppendPut(seq uint64, key, value []byte) error {
	return w.appendRecord(seq, recordPut, key, value)
}

// AppendDelete appends a DELETE (tombstone) record.
func (w *WAL) AppendDelete(seq uint64, key []byte) error {
	return w.appendRecord(seq, recordDelete, key, nil)
}

func (w *WAL) appendRecord(seq uint64, typ byte, key, value []byte) error {
	buf := make([]byte, 8+4+4+1+len(key)+len(value))
	off := 0

	binary.BigEndian.PutUint64(buf[off:], seq)
	off += 8

	binary.BigEndian.PutUint32(buf[off:], uint32(len(key)))
	off += 4

	binary.BigEndian.PutUint32(buf[off:], uint32(len(value)))
	off += 4

	buf[off] = typ
	off++

	copy(buf[off:], key)
	off += len(key)

	copy(buf[off:], value)

	if _, err := w.file.Write(buf); err != nil {
		return err
	}

	return w.file.Sync()
}

// Entry represents a replayed WAL record.
type Entry struct {
	Seq       uint64
	Key       []byte
	Value     []byte
	Tombstone bool
}

// Replay replays WAL records with seq > fromSeq.
func (w *WAL) Replay(fromSeq uint64) ([]Entry, error) {
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	var entries []Entry

	for {
		var seq uint64
		if err := binary.Read(w.file, binary.BigEndian, &seq); err != nil {
			if err == io.EOF {
				break
			}
			return nil, err
		}

		var keyLen uint32
		var valLen uint32

		if err := binary.Read(w.file, binary.BigEndian, &keyLen); err != nil {
			return nil, err
		}
		if err := binary.Read(w.file, binary.BigEndian, &valLen); err != nil {
			return nil, err
		}

		typ := make([]byte, 1)
		if _, err := io.ReadFull(w.file, typ); err != nil {
			return nil, err
		}

		key := make([]byte, keyLen)
		if _, err := io.ReadFull(w.file, key); err != nil {
			return nil, err
		}

		value := make([]byte, valLen)
		if _, err := io.ReadFull(w.file, value); err != nil {
			return nil, err
		}

		if seq > fromSeq {
			entries = append(entries, Entry{
				Seq:       seq,
				Key:       key,
				Value:     value,
				Tombstone: typ[0] == recordDelete,
			})
		}
	}

	return entries, nil
}

// Close closes the WAL file.
func (w *WAL) Close() error {
	return w.file.Close()
}
