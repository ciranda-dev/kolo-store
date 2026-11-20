package kolostore

import (
	"bytes"
	"errors"
	"slices"
	"sync"
)

var (
	// ErrNotFound is returned by [Backend.Get] for a key that is not stored.
	ErrNotFound = errors.New("kolostore: not found")
	// ErrClosed is returned after Close by a backend, a store or a log.
	ErrClosed = errors.New("kolostore: closed")
	// ErrTooLarge is returned for a write too large for one batch: a Pebble batch of 4 GiB - 1 bytes or more (2 GiB - 1 on 32-bit), or a [Store.Adopt] batch of 2 GiB or more (1 GiB).
	ErrTooLarge = errors.New("kolostore: batch too large")
)

var errBatchUsed = errors.New("kolostore: batch already committed")

// Backend is the ordered key-value storage that a [Store] runs on; returned slices are copies.
// Use a comparable type, such as a pointer, so that Stores on one backend share their open logs.
type Backend interface {
	// Get returns the value of key, or [ErrNotFound].
	Get(key []byte) ([]byte, error)
	// Put durably stores value under key.
	Put(key, value []byte) error
	// Delete durably removes key; deleting a missing key is not an error.
	Delete(key []byte) error
	// Scan calls fn for each key in [start, end) in order until fn returns false.
	// A nil end means no upper bound and limit <= 0 means no limit; fn must not call the backend.
	Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error
	// NewBatch returns an empty batch of writes.
	NewBatch() Batch
	// Close releases the backend; later calls return [ErrClosed] and a second Close returns nil.
	Close() error
}

// Batch collects writes that [Batch.Commit] applies atomically and durably.
type Batch interface {
	// Put adds a write of value under key; the batch keeps its own copy, so the caller may clear value after Commit.
	Put(key, value []byte)
	// Delete adds a removal of key.
	Delete(key []byte)
	// DeleteRange adds a removal of every key in [start, end); an empty range does nothing.
	DeleteRange(start, end []byte)
	// Commit applies every write or none; a batch can be committed once.
	Commit() error
}

type kv struct {
	key, value []byte
}

type memory struct {
	mu     sync.RWMutex
	data   []kv // sorted by key; stored slices are never modified
	closed bool
}

// NewMemory returns an empty in-memory [Backend], for tests and short-lived stores.
func NewMemory() Backend {
	return &memory{}
}

func (m *memory) find(key []byte) (int, bool) {
	return slices.BinarySearchFunc(m.data, key, func(e kv, k []byte) int { return bytes.Compare(e.key, k) })
}

func (m *memory) Get(key []byte) ([]byte, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	if m.closed {
		return nil, ErrClosed
	}
	i, ok := m.find(key)
	if !ok {
		return nil, ErrNotFound
	}
	return clone(m.data[i].value), nil
}

func (m *memory) Put(key, value []byte) error {
	b := m.NewBatch()
	b.Put(key, value)
	return b.Commit()
}

func (m *memory) Delete(key []byte) error {
	b := m.NewBatch()
	b.Delete(key)
	return b.Commit()
}

func (m *memory) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	m.mu.RLock()
	if m.closed {
		m.mu.RUnlock()
		return ErrClosed
	}
	lo, _ := m.find(start)
	hi := len(m.data)
	if end != nil {
		hi, _ = m.find(end)
	}
	var snap []kv
	if lo < hi {
		snap = slices.Clone(m.data[lo:hi])
	}
	m.mu.RUnlock()

	if reverse {
		slices.Reverse(snap)
	}
	if limit > 0 && len(snap) > limit {
		snap = snap[:limit]
	}
	for _, e := range snap {
		if !fn(clone(e.key), clone(e.value)) {
			break
		}
	}
	return nil
}

func (m *memory) NewBatch() Batch {
	return &memoryBatch{m: m}
}

func (m *memory) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	m.data = nil
	return nil
}

type opKind uint8

const (
	opPut opKind = iota
	opDelete
	opDeleteRange
)

type op struct {
	kind       opKind
	key, value []byte // value is the range end for opDeleteRange
}

// ops records batch writes as copies; each backend applies them at Commit.
type ops struct {
	list []op
	used bool
}

func (b *ops) Put(key, value []byte) {
	b.list = append(b.list, op{opPut, clone(key), clone(value)})
}

func (b *ops) Delete(key []byte) {
	b.list = append(b.list, op{opDelete, clone(key), nil})
}

func (b *ops) DeleteRange(start, end []byte) {
	b.list = append(b.list, op{opDeleteRange, clone(start), clone(end)})
}

type memoryBatch struct {
	ops
	m *memory
}

func (b *memoryBatch) Commit() error {
	if b.used {
		return errBatchUsed
	}
	m := b.m
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closed {
		return ErrClosed
	}
	b.used = true
	for _, o := range b.list {
		switch o.kind {
		case opPut:
			i, ok := m.find(o.key)
			if ok {
				m.data[i].value = o.value
			} else {
				m.data = slices.Insert(m.data, i, kv{o.key, o.value})
			}
		case opDelete:
			if i, ok := m.find(o.key); ok {
				m.data = slices.Delete(m.data, i, i+1)
			}
		case opDeleteRange:
			if bytes.Compare(o.key, o.value) >= 0 {
				continue
			}
			lo, _ := m.find(o.key)
			hi, _ := m.find(o.value)
			m.data = slices.Delete(m.data, lo, hi)
		}
	}
	b.list = nil
	return nil
}

// clone returns a non-nil copy, so an empty stored value reads back as empty, not nil.
func clone(b []byte) []byte {
	return append([]byte{}, b...)
}
