package kolostore

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"slices"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// ErrIncomplete is returned by [Store.Adopt] for a replica that has no head yet or an unfinished catch-up.
var ErrIncomplete = errors.New("kolostore: replica is not complete")

var errNilReplica = errors.New("kolostore: nil replica")

// adoptLimit is the batch size, counted like a Pebble batch, from which Adopt refuses: Adopt and Pebble each hold a copy, so both fit in Pebble's limit.
var adoptLimit = (pebbleBatchLimit + 1) / 2

// Adopt brings the closed own log of id stored here up to rep, a replica of it with the same header in any store, and opens it with opts like [Store.Open].
// rep needs a head, no pending catch-up ([ErrIncomplete]) and the keys of its live entries past the stored head, which must be a prefix of its head ([kololog.ErrRollback]).
// One batch adds those entries and rep's head and keeps every deletion of either side, so a crash leaves the old log or the new one; a batch of 2 GiB or more (1 GiB on 32-bit) returns [ErrTooLarge].
// An error from the final open leaves the adopted log stored, and [Store.Open] opens it.
func (s *Store) Adopt(id *koloid.Identity, rep *Replica, opts ...kololog.Option) (*Log, error) {
	end, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer end()
	if rep == nil {
		return nil, errNilReplica
	}
	pub := id.PublicKey()
	if len(pub) != ed25519.PublicKeySize {
		return nil, fmt.Errorf("kolostore: %w", koloid.ErrInvalidIdentity)
	}
	logID := rep.ID()
	if !bytes.Equal(rep.Header().LogKey, pub) {
		return nil, fmt.Errorf("kolostore: adopt log %s: %w: replica belongs to another owner", logID, kololog.ErrBadLogHeader)
	}
	head, err := completeHead(rep)
	if err != nil {
		return nil, fmt.Errorf("kolostore: adopt log %s: %w", logID, err)
	}
	if err := s.reserve(logID); err != nil {
		return nil, err
	}
	takeOver, unreserve, err := kololog.ReserveForRestore(pub, logID)
	if err != nil {
		s.release(logID)
		return nil, fmt.Errorf("%w: %s: %w", ErrLogInUse, logID, err)
	}
	defer unreserve()
	if err := s.extend(pub, rep, head); err != nil {
		s.release(logID)
		return nil, fmt.Errorf("kolostore: adopt log %s: %w", logID, err)
	}
	kl, err := s.load(id, pub, logID, append(slices.Clip(opts), takeOver))
	if err != nil {
		s.release(logID)
		return nil, fmt.Errorf("kolostore: open adopted log %s: %w", logID, err)
	}
	l := &Log{s: s, log: kl, id: logID}
	s.track(l)
	return l, nil
}

// completeHead returns the head of rep, or [ErrIncomplete] if rep has none or a pending target; without a target rep holds every entry of its head.
func completeHead(rep *Replica) (kololog.Head, error) {
	head, err := rep.Head()
	if errors.Is(err, kololog.ErrNoHead) {
		return kololog.Head{}, fmt.Errorf("%w: no head", ErrIncomplete)
	}
	if err != nil {
		return kololog.Head{}, err
	}
	if _, _, ok := rep.Target(); ok {
		return kololog.Head{}, fmt.Errorf("%w: catch-up unfinished", ErrIncomplete)
	}
	return head, nil
}

// extend checks the stored own log against rep and writes, in one batch, rep's entries past it, the deletions only rep holds and rep's head.
func (s *Store) extend(pub ed25519.PublicKey, rep *Replica, head kololog.Head) error {
	logID := rep.ID()
	st, err := s.read(pub, logID, false)
	if err != nil {
		return err
	}
	if !bytes.Equal(rep.Header().Encode(), st.header) {
		return fmt.Errorf("%w: replica header differs from the stored one", kololog.ErrBadLogHeader)
	}
	old := st.headSize
	if err := checkPrefix(pub, rep, *st.heads.Head, head); err != nil {
		return err
	}
	repDeleted, storeDeleted, err := s.deletions(logID, rep, old)
	if err != nil {
		return err
	}
	for _, i := range storeDeleted {
		if err := s.checkMarker(pub, rep, i); err != nil {
			return err
		}
	}
	b := &boundedBatch{b: s.b.NewBatch()}
	for _, i := range repDeleted {
		if err := putEntry(b, rep, i); err != nil {
			return err
		}
		if err := b.delete(layout.KeyKey(logID, i)); err != nil {
			return err
		}
	}
	for i := old; i < head.Size; i++ {
		if err := putEntry(b, rep, i); err != nil {
			return err
		}
	}
	if err := b.put(layout.HeadKey(logID), head.Encode()); err != nil {
		return err
	}
	return b.b.Commit()
}

// checkPrefix returns [kololog.ErrRollback] unless the stored head is a prefix of rep's head.
func checkPrefix(pub ed25519.PublicKey, rep *Replica, oldHead, newHead kololog.Head) error {
	o, err := kololog.VerifyHead(pub, rep.ID(), oldHead)
	if err != nil {
		return corrupt(err)
	}
	n, err := kololog.VerifyHead(pub, rep.ID(), newHead)
	if err != nil {
		return err
	}
	if o.Size() > n.Size() {
		return fmt.Errorf("%w: stored log has %d entries, replica %d", kololog.ErrRollback, o.Size(), n.Size())
	}
	if o.Size() == 0 {
		return nil
	}
	p, err := rep.ConsistencyProof(o.Size(), n.Size())
	if err != nil {
		return err
	}
	if !kololog.VerifyConsistency(o, n, p) {
		return fmt.Errorf("%w: stored log is not a prefix of the replica", kololog.ErrRollback)
	}
	return nil
}

// deletions compares the first n entries of the stored own log, where an entry is live exactly when its key is stored, with rep.
// It returns the entries only rep has deleted and those only the store has deleted, scanning the stored keys and no ciphertext.
func (s *Store) deletions(logID kololog.LogID, rep *Replica, n uint64) (repOnly, storeOnly []uint64, err error) {
	var next uint64
	var bad error
	visit := func(i uint64, live bool) bool {
		info, err := rep.EntryInfo(i)
		if err != nil {
			bad = err
			return false
		}
		switch {
		case live && info.Deleted:
			repOnly = append(repOnly, i)
		case !live && !info.Deleted:
			storeOnly = append(storeOnly, i)
		}
		return true
	}
	err = s.b.Scan(layout.KeyKey(logID, 0), layout.KeyKey(logID, n), false, 0, func(k, v []byte) bool {
		clear(v)
		for ; next < n && bytes.Compare(k, layout.KeyKey(logID, next)) > 0; next++ {
			if !visit(next, false) {
				return false
			}
		}
		if next == n || !bytes.Equal(k, layout.KeyKey(logID, next)) {
			bad = fmt.Errorf("%w: unexpected key %x", ErrCorrupt, k)
			return false
		}
		next++
		return visit(next-1, true)
	})
	for ; err == nil && bad == nil && next < n; next++ {
		visit(next, false)
	}
	if err == nil {
		err = bad
	}
	return repOnly, storeOnly, err
}

// checkMarker refuses an entry the store deleted unless its stored marker is the owner's for rep's record, so the adopted log opens.
func (s *Store) checkMarker(pub ed25519.PublicKey, rep *Replica, i uint64) error {
	v, err := s.b.Get(layout.EntryKey(rep.ID(), i))
	if errors.Is(err, ErrNotFound) {
		return fmt.Errorf("%w: entry %d missing", ErrCorrupt, i)
	}
	if err != nil {
		return err
	}
	e, err := decodeEntry(v)
	if err != nil {
		return err
	}
	r, err := rep.Record(i)
	if err != nil {
		return err
	}
	m, err := kololog.DecodeDeleteMarker(e.Marker)
	if err != nil || !bytes.Equal(e.Record, r.Encode()) || !kololog.VerifyDeleteMarker(pub, rep.ID(), r, m) {
		return fmt.Errorf("%w: entry %d has no key and no valid delete marker", ErrCorrupt, i)
	}
	return nil
}

func putEntry(b *boundedBatch, rep *Replica, index uint64) error {
	info, err := rep.EntryInfo(index)
	if err != nil {
		return err
	}
	if !info.Deleted && !info.Keyed {
		return fmt.Errorf("entry %d: %w", index, kololog.ErrKeyUnavailable)
	}
	e, err := rep.Entry(index)
	if err != nil {
		return err
	}
	defer clear(e.Key)
	id := rep.ID()
	if err := b.put(layout.EntryKey(id, index), layout.EncodeEntry(e)); err != nil {
		return err
	}
	if len(e.Key) > 0 {
		return b.put(layout.KeyKey(id, index), e.Key)
	}
	return nil
}

// boundedBatch returns [ErrTooLarge] instead of growing to adoptLimit.
type boundedBatch struct {
	b    Batch
	size uint64
}

func (b *boundedBatch) grow(key, value []byte) error {
	b.size += batchCost(key, value)
	if b.size >= adoptLimit {
		return fmt.Errorf("%w: adopt needs %d bytes or more in one batch", ErrTooLarge, adoptLimit)
	}
	return nil
}

func (b *boundedBatch) put(key, value []byte) error {
	if err := b.grow(key, value); err != nil {
		return err
	}
	b.b.Put(key, value)
	return nil
}

func (b *boundedBatch) delete(key []byte) error {
	if err := b.grow(key, nil); err != nil {
		return err
	}
	b.b.Delete(key)
	return nil
}
