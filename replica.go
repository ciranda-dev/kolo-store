package kolostore

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"sync"

	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// OpenReplica opens the stored read-only replica of the log logID owned by pub, or creates it from the signed header if the log is not stored.
// An opened replica is verified like [Store.Open]; a non-empty header must then equal the stored one. An own log returns [ErrNotReplica].
func (s *Store) OpenReplica(pub ed25519.PublicKey, logID kololog.LogID, header []byte) (*Replica, error) {
	end, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer end()
	if len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("kolostore: %w: public key of %d bytes", kololog.ErrMalformed, len(pub))
	}
	if err := s.reserve(logID); err != nil {
		return nil, err
	}
	kr, err := s.loadReplica(pub, logID, header)
	if err != nil {
		s.release(logID)
		return nil, fmt.Errorf("kolostore: open replica %s: %w", logID, err)
	}
	r := &Replica{s: s, id: logID, r: kr}
	s.track(r)
	return r, nil
}

func (s *Store) loadReplica(pub ed25519.PublicKey, logID kololog.LogID, header []byte) (*kololog.Replica, error) {
	_, err := s.b.Get(layout.ListKey(logID))
	if errors.Is(err, ErrNotFound) {
		return s.createReplica(pub, logID, header)
	}
	if err != nil {
		return nil, err
	}
	st, err := s.read(pub, logID, true)
	if err != nil {
		return nil, err
	}
	if len(header) > 0 && !bytes.Equal(header, st.header) {
		return nil, fmt.Errorf("%w: header differs from the stored one", kololog.ErrBadLogHeader)
	}
	sto := &logStorage{s: s, id: logID}
	sto.startScan(st.size)
	defer sto.stopScan()
	// An unfinished catch-up is replayed through the checks of Update, so it is verified the same way as when it arrived.
	kr, err := kololog.RestoreReplica(pub, logID, st.header, st.heads, sto)
	if errors.Is(err, kololog.ErrRestoreMismatch) {
		return nil, corrupt(err)
	}
	if err != nil {
		return nil, err
	}
	if err := sto.endScan(); err != nil {
		return nil, err
	}
	return kr, nil
}

// createReplica refuses a log ID that has keys but is not listed, since that is a damaged log, not a new one.
func (s *Store) createReplica(pub ed25519.PublicKey, logID kololog.LogID, header []byte) (*kololog.Replica, error) {
	var unexpected []byte
	if err := s.b.Scan(layout.LogPrefix(logID), layout.LogPrefixEnd(logID), false, 1, func(k, _ []byte) bool {
		unexpected = k
		return false
	}); err != nil {
		return nil, err
	}
	if unexpected != nil {
		return nil, fmt.Errorf("%w: log not listed", ErrCorrupt)
	}
	kr, err := kololog.NewReplica(pub, logID, header, &logStorage{s: s, id: logID})
	if err != nil {
		return nil, err
	}
	b := s.b.NewBatch()
	b.Put(layout.HeaderKey(logID), kr.Header().Encode())
	b.Put(layout.ListKey(logID), layout.ReplicaKind)
	if err := b.Commit(); err != nil {
		return nil, err
	}
	return kr, nil
}

// Replica is an open read-only replica of a [Store]; it is safe for concurrent use.
// Every change is one atomic batch that kolo-log commits before it shows the change, so a failed write changes nothing.
type Replica struct {
	s  *Store
	id kololog.LogID
	r  *kololog.Replica

	mu     sync.RWMutex
	closed bool
}

func (r *Replica) check() error {
	if r.closed {
		return ErrClosed
	}
	return nil
}

// Update grows the replica with [kololog.Replica.Update], which stores the new entries and keys, the head and any unfinished
// target with its proof in one batch. The entries may be a chunk that stops short of h; OpenReplica replays a stored target.
// A refused update or a failed write changes nothing.
func (r *Replica) Update(h kololog.Head, p kololog.ConsistencyProof, entries []kololog.Entry) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return err
	}
	return r.r.Update(h, p, entries)
}

// AddKey gives an entry that came without its key the key with [kololog.Replica.AddKey] and stores it in one write;
// the key it already has writes nothing.
func (r *Replica) AddKey(index uint64, key []byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return err
	}
	return fromStorage(r.r.AddKey(index, key))
}

// AddKeys gives the entries from start on that came without their keys the keys with [kololog.Replica.AddKeys] and stores
// every key that passes in one batch; a failed write stores none.
func (r *Replica) AddKeys(start uint64, keys []kololog.EntryKey) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return err
	}
	return fromStorage(r.r.AddKeys(start, keys))
}

// ApplyDelete applies delete markers with [kololog.Replica.ApplyDelete], storing each in place of its entry's ciphertext and
// removing its key, all in one batch; one bad marker applies none. As with [Log.Delete], old copies of the disk keep a working key.
func (r *Replica) ApplyDelete(markers ...[]byte) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return err
	}
	return r.r.ApplyDelete(markers...)
}

// Head returns the last verified head, as received, or [kololog.ErrNoHead].
func (r *Replica) Head() (kololog.Head, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return kololog.Head{}, err
	}
	return r.r.Head()
}

// Target returns the head an unfinished catch-up is filling and the proof from Len to it, or ok false; see [kololog.Replica.Target].
func (r *Replica) Target() (h kololog.Head, p kololog.ConsistencyProof, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.check() != nil {
		return kololog.Head{}, kololog.ConsistencyProof{}, false
	}
	return r.r.Target()
}

// Read decodes the entry at index into out.
func (r *Replica) Read(index uint64, out any) error {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return err
	}
	return fromStorage(r.r.Read(index, out))
}

// Get returns the stored bytes of the entry at index.
func (r *Replica) Get(index uint64) ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return nil, err
	}
	v, err := r.r.Get(index)
	return v, fromStorage(err)
}

// Entry returns the stored form of the entry at index, its key included; clear Key when done with it.
func (r *Replica) Entry(index uint64) (kololog.Entry, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return kololog.Entry{}, err
	}
	e, err := r.r.Entry(index)
	return e, fromStorage(err)
}

// Key returns the key of the entry at index without reading its ciphertext; clear it when done.
func (r *Replica) Key(index uint64) (kololog.EntryKey, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return nil, err
	}
	k, err := r.r.Key(index)
	return k, fromStorage(err)
}

// EntryInfo returns whether the entry at index is deleted and whether its key is stored, without reading the backend.
func (r *Replica) EntryInfo(index uint64) (kololog.EntryInfo, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return kololog.EntryInfo{}, err
	}
	return r.r.EntryInfo(index)
}

// Record returns the public record of the entry at index.
func (r *Replica) Record(index uint64) (kololog.Record, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return kololog.Record{}, err
	}
	return r.r.Record(index)
}

// InclusionProof returns a proof that the entry at index is in the tree of the first size entries.
func (r *Replica) InclusionProof(index, size uint64) (kololog.InclusionProof, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return kololog.InclusionProof{}, err
	}
	return r.r.InclusionProof(index, size)
}

// ConsistencyProof returns a proof that the tree of oldSize entries is a prefix of the tree of newSize entries.
func (r *Replica) ConsistencyProof(oldSize, newSize uint64) (kololog.ConsistencyProof, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if err := r.check(); err != nil {
		return kololog.ConsistencyProof{}, err
	}
	return r.r.ConsistencyProof(oldSize, newSize)
}

// Header returns a copy of the signed log header; it also works after Close.
func (r *Replica) Header() kololog.LogHeader { return r.r.Header() }

// ID returns the log ID; it also works after Close.
func (r *Replica) ID() kololog.LogID { return r.id }

// Len returns the number of stored entries, deleted ones included; after Close it returns the last length.
func (r *Replica) Len() uint64 { return r.r.Len() }

// Close releases the replica so it can be opened again; a second Close returns nil.
func (r *Replica) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil
	}
	r.closed = true
	err := r.r.Close()
	r.s.untrack(r)
	return err
}
