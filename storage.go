package kolostore

import (
	"bytes"
	"errors"
	"fmt"
	"iter"
	"slices"

	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// logStorage is the kololog.Storage of one stored log; kolo-log reads entries here.
// An entry's key is stored under its own backend key, so reading a key never reads the ciphertext.
// While Open restores the log, entries come from one scan of each range, so each value is read once and any stray key is caught.
type logStorage struct {
	s    *Store
	id   kololog.LogID
	scan *scan
}

type scan struct {
	entries, keys *puller
	index         uint64
}

// puller reads one backend range in order, one pair at a time.
type puller struct {
	next   func() ([]byte, []byte, bool)
	stop   func()
	err    error // from Backend.Scan once it returns
	k, v   []byte
	ok     bool
	peeked bool
}

func pull(b Backend, start, end []byte) *puller {
	p := &puller{}
	p.next, p.stop = iter.Pull2(func(yield func([]byte, []byte) bool) {
		p.err = b.Scan(start, end, false, 0, yield)
	})
	return p
}

// peek returns the next pair without taking it.
func (p *puller) peek() (k, v []byte, ok bool) {
	if !p.peeked {
		p.k, p.v, p.ok = p.next()
		p.peeked = true
	}
	return p.k, p.v, p.ok
}

func (p *puller) take() {
	p.peeked = false
}

// end returns an error for a pair left in the range, or the scan's error.
func (p *puller) end() error {
	if k, v, ok := p.peek(); ok {
		clear(v)
		return fmt.Errorf("%w: unexpected key %x", ErrCorrupt, k)
	}
	return p.err
}

func (st *logStorage) Entry(index uint64) (kololog.Entry, error) {
	var e kololog.Entry
	var err error
	if st.scan != nil {
		e, err = st.scan.entry(st.id, index)
	} else {
		e, err = st.get(index)
	}
	if err != nil {
		return kololog.Entry{}, fmt.Errorf("entry %d: %w", index, err)
	}
	if f := entryDecoded.get(); f != nil {
		f(e)
	}
	return e, nil
}

func (st *logStorage) get(index uint64) (kololog.Entry, error) {
	v, err := st.s.b.Get(layout.EntryKey(st.id, index))
	if errors.Is(err, ErrNotFound) {
		// kololog.ErrOutOfRange is the Storage answer for "no entry here"; a log that should hold one sees damage.
		return kololog.Entry{}, fmt.Errorf("%w: %w: entry missing", ErrCorrupt, kololog.ErrOutOfRange)
	}
	if err != nil {
		return kololog.Entry{}, err
	}
	e, err := decodeEntry(v)
	if err != nil {
		return kololog.Entry{}, err
	}
	if e.Key, err = st.key(index); err != nil {
		return kololog.Entry{}, err
	}
	return e, nil
}

// Key returns the stored key of entry index, nil when none is stored.
func (st *logStorage) Key(index uint64) (kololog.EntryKey, error) {
	k, err := st.key(index)
	if err != nil {
		return nil, fmt.Errorf("entry %d: %w", index, err)
	}
	return k, nil
}

func (st *logStorage) key(index uint64) (kololog.EntryKey, error) {
	v, err := st.s.b.Get(layout.KeyKey(st.id, index))
	if errors.Is(err, ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	defer clear(v)
	return decodeKey(v)
}

func (sc *scan) entry(id kololog.LogID, index uint64) (kololog.Entry, error) {
	if index != sc.index {
		return kololog.Entry{}, fmt.Errorf("kolostore: entry %d read during Open out of order, want %d", index, sc.index)
	}
	k, v, ok := sc.entries.peek()
	switch {
	case !ok && sc.entries.err != nil:
		return kololog.Entry{}, sc.entries.err
	case !ok:
		return kololog.Entry{}, fmt.Errorf("%w: entry missing", ErrCorrupt)
	case !bytes.Equal(k, layout.EntryKey(id, index)):
		return kololog.Entry{}, fmt.Errorf("%w: entry missing or key %x unexpected", ErrCorrupt, k)
	}
	sc.entries.take()
	e, err := decodeEntry(v)
	if err != nil {
		return kololog.Entry{}, err
	}
	want := layout.KeyKey(id, index)
	k, v, ok = sc.keys.peek()
	switch {
	case !ok && sc.keys.err != nil:
		return kololog.Entry{}, sc.keys.err
	case !ok || bytes.Compare(k, want) > 0:
		// No key stored for this entry.
	case !bytes.Equal(k, want):
		clear(v)
		return kololog.Entry{}, fmt.Errorf("%w: unexpected key %x", ErrCorrupt, k)
	default:
		sc.keys.take()
		e.Key, err = decodeKey(v)
		clear(v)
		if err != nil {
			return kololog.Entry{}, err
		}
	}
	sc.index++
	return e, nil
}

// startScan makes Entry read the first n entries and their keys, in order, from one scan of each range.
func (st *logStorage) startScan(n uint64) {
	st.scan = &scan{
		entries: pull(st.s.b, layout.EntryKey(st.id, 0), layout.EntryKey(st.id, n)),
		keys:    pull(st.s.b, layout.KeyKey(st.id, 0), layout.KeyKey(st.id, n)),
	}
}

// endScan checks that both scans hold no pair past the entries read, then switches Entry to point reads.
func (st *logStorage) endScan() error {
	sc := st.scan
	defer st.stopScan()
	return errors.Join(sc.entries.end(), sc.keys.end())
}

// stopScan ends an unfinished scan; it does nothing after endScan.
func (st *logStorage) stopScan() {
	if st.scan != nil {
		st.scan.entries.stop()
		st.scan.keys.stop()
		st.scan = nil
	}
}

// Commit writes c in one atomic, durable batch: entry values, keys, delete markers and heads.
func (st *logStorage) Commit(c kololog.Change) error {
	if err := checkKeys(c); err != nil {
		return err
	}
	end, err := st.s.begin()
	if err != nil {
		return err
	}
	defer end()
	b := st.s.b.NewBatch()
	var values [][]byte
	defer func() {
		for _, v := range values {
			clear(v)
		}
	}()
	put := func(k, v []byte) {
		values = append(values, v)
		b.Put(k, v)
	}
	for i, e := range c.Entries {
		index := c.Start + uint64(i)
		put(layout.EntryKey(st.id, index), layout.EncodeEntry(e))
		if len(e.Key) > 0 {
			put(layout.KeyKey(st.id, index), slices.Clone(e.Key))
		}
	}
	for _, del := range c.Deleted {
		put(layout.EntryKey(st.id, del.Index), layout.EncodeEntry(kololog.Entry{Record: del.Record, Marker: del.Marker}))
		b.Delete(layout.KeyKey(st.id, del.Index))
	}
	for _, k := range c.Keys {
		put(layout.KeyKey(st.id, k.Index), slices.Clone(k.Key))
	}
	if h := c.Heads; h != nil {
		if h.Head != nil {
			put(layout.HeadKey(st.id), h.Head.Encode())
		}
		if h.Target != nil {
			put(layout.TargetKey(st.id), layout.EncodeTarget(h.Target.Encode(), h.Proof.Encode()))
		} else {
			b.Delete(layout.TargetKey(st.id))
		}
	}
	return b.Commit()
}

// checkKeys refuses the keys of c that a log never commits, since a stored stray key would later read as the key of an entry:
// a key of the wrong size, for an entry not stored before c (new entries carry their own), or out of ascending index order.
func checkKeys(c kololog.Change) error {
	for i, k := range c.Keys {
		switch {
		case len(k.Key) != layout.KeySize:
			return fmt.Errorf("kolostore: key of %d bytes for entry %d", len(k.Key), k.Index)
		case k.Index >= c.Start:
			return fmt.Errorf("kolostore: key for entry %d of %d: %w", k.Index, c.Start, kololog.ErrOutOfRange)
		case i > 0 && k.Index <= c.Keys[i-1].Index:
			return fmt.Errorf("kolostore: key for entry %d out of order", k.Index)
		}
	}
	return nil
}

// fromStorage marks a read that found the stored entry changed since it was verified as damaged storage.
func fromStorage(err error) error {
	if errors.Is(err, kololog.ErrStorageMismatch) {
		return corrupt(err)
	}
	return err
}

func decodeKey(v []byte) (kololog.EntryKey, error) {
	k, err := layout.DecodeKey(v)
	if err != nil {
		return nil, corrupt(err)
	}
	return k, nil
}
