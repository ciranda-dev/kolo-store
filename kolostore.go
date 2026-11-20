package kolostore

import (
	"bytes"
	"crypto/ed25519"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"sync"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// ErrCorrupt is returned when stored data is missing, extra, cannot be decoded or does not verify.
var ErrCorrupt = errors.New("kolostore: corrupt data")

var errNilBackend = errors.New("kolostore: nil backend")

var (
	// ErrReplica is returned by [Store.Open] for a stored read-only replica, which only [Store.OpenReplica] opens.
	ErrReplica = errors.New("kolostore: log is a read-only replica")
	// ErrNotReplica is returned by [Store.OpenReplica] for a stored log that is not a replica, such as one of your own.
	ErrNotReplica = errors.New("kolostore: log is not a replica")
	// ErrLogInUse is returned when a log is already open on the same backend in this process.
	ErrLogInUse = errors.New("kolostore: log already open")
)

// logClosing lets tests make a Log's Close panic.
var logClosing hook[func(*Log)]

// beforeRestore lets tests act between the scan and Restore in Open.
var beforeRestore hook[func()]

// entryDecoded lets tests see which stored entries the store decodes.
var entryDecoded hook[func(kololog.Entry)]

// Store persists many kolo-log logs in one [Backend].
type Store struct {
	b   Backend
	key any // identifies the backend in openLogs

	mu        sync.RWMutex // held for reading by every backend write, so Close waits for them
	closed    bool
	closeOnce sync.Once
	closeErr  error

	logsMu sync.Mutex
	logs   map[handle]bool
}

// handle is an open *Log or *Replica.
type handle interface {
	ID() kololog.LogID
	Close() error
}

type openKey struct {
	backend any
	logID   kololog.LogID
}

// openLogs is process-wide, so two Stores on one backend never open the same log twice.
var openLogs = struct {
	sync.Mutex
	m map[openKey]bool
}{m: make(map[openKey]bool)}

// New returns a Store on b; [Store.Close] closes b. With a nil b, also a nil pointer, every method returns an error and Close returns nil.
// Stores on one comparable backend value share their open logs; a non-comparable backend only guards its own Store.
func New(b Backend) *Store {
	if isNil(b) {
		b = nil
	}
	s := &Store{b: b, logs: make(map[handle]bool)}
	s.key = s
	if b != nil && reflect.ValueOf(b).Comparable() {
		s.key = b
	}
	return s
}

func isNil(b Backend) bool {
	if b == nil {
		return true
	}
	v := reflect.ValueOf(b)
	switch v.Kind() {
	case reflect.Pointer, reflect.Map, reflect.Slice, reflect.Func, reflect.Chan, reflect.Interface, reflect.UnsafePointer:
		return v.IsNil()
	}
	return false
}

// begin returns a function that ends the backend operation it guards.
func (s *Store) begin() (func(), error) {
	if s.b == nil {
		return nil, errNilBackend
	}
	s.mu.RLock()
	if s.closed {
		s.mu.RUnlock()
		return nil, ErrClosed
	}
	return s.mu.RUnlock, nil
}

func (s *Store) reserve(id kololog.LogID) error {
	openLogs.Lock()
	defer openLogs.Unlock()
	k := openKey{s.key, id}
	if openLogs.m[k] {
		return fmt.Errorf("%w: %s", ErrLogInUse, id)
	}
	openLogs.m[k] = true
	return nil
}

func (s *Store) release(id kololog.LogID) {
	openLogs.Lock()
	defer openLogs.Unlock()
	delete(openLogs.m, openKey{s.key, id})
}

// Create makes a new log with kololog.New and durably stores its header and empty head in one batch.
func (s *Store) Create(id *koloid.Identity, opts ...kololog.Option) (*Log, error) {
	end, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer end()
	st := &logStorage{s: s}
	kl, err := kololog.New(id, st, opts...)
	if err != nil {
		return nil, err
	}
	logID := kl.ID()
	st.id = logID
	if err := s.reserve(logID); err != nil {
		kl.Close()
		return nil, err
	}
	head, err := kl.Head()
	if err == nil {
		b := s.b.NewBatch()
		b.Put(layout.HeaderKey(logID), kl.Header().Encode())
		b.Put(layout.HeadKey(logID), head.Encode())
		b.Put(layout.ListKey(logID), nil)
		err = b.Commit()
	}
	if err != nil {
		kl.Close()
		s.release(logID)
		return nil, fmt.Errorf("kolostore: create log %s: %w", logID, err)
	}
	l := &Log{s: s, log: kl, id: logID}
	s.track(l)
	return l, nil
}

// Open loads a log from the backend and restores it with kololog.Restore, which verifies every entry.
// Pass kololog.WithTrustedHead with the last head you published to reject an older snapshot (kololog.ErrRollback);
// the storage is trusted to be the node's own disk, so that head is the only defense against a rollback.
// Damaged data returns an error wrapping [ErrCorrupt] or a kololog error, and a stored replica returns [ErrReplica].
func (s *Store) Open(id *koloid.Identity, logID kololog.LogID, opts ...kololog.Option) (*Log, error) {
	end, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer end()
	if err := s.reserve(logID); err != nil {
		return nil, err
	}
	pub := id.PublicKey()
	if len(pub) != ed25519.PublicKeySize {
		s.release(logID)
		return nil, fmt.Errorf("kolostore: %w", koloid.ErrInvalidIdentity)
	}
	takeOver, unreserve, err := kololog.ReserveForRestore(pub, logID)
	if err != nil {
		s.release(logID)
		return nil, fmt.Errorf("%w: %s: %w", ErrLogInUse, logID, err)
	}
	defer unreserve()
	kl, err := s.load(id, pub, logID, append(slices.Clip(opts), takeOver))
	if err != nil {
		s.release(logID)
		return nil, fmt.Errorf("kolostore: open log %s: %w", logID, err)
	}
	l := &Log{s: s, log: kl, id: logID}
	s.track(l)
	return l, nil
}

func (s *Store) load(id *koloid.Identity, pub ed25519.PublicKey, logID kololog.LogID, opts []kololog.Option) (*kololog.Log, error) {
	st, err := s.read(pub, logID, false)
	if err != nil {
		return nil, err
	}
	sto := &logStorage{s: s, id: logID}
	sto.startScan(st.size)
	defer sto.stopScan()
	kl, err := kololog.Restore(id, st.header, st.head, sto, opts...)
	if errors.Is(err, kololog.ErrRestoreMismatch) {
		return nil, corrupt(err)
	}
	if err != nil {
		return nil, err
	}
	if err := sto.endScan(); err != nil {
		kl.Close()
		return nil, err
	}
	return kl, nil
}

// stored is a stored log's header and heads, checked against the owner key, and its number of stored entries.
type stored struct {
	header, head []byte
	heads        kololog.Heads // decoded head, and a replica's unfinished catch-up
	headSize     uint64
	size         uint64 // headSize entries under head, the rest under the target
}

// read loads a stored log's header and heads and checks them against pub, and that no key other than its entries
// below size is stored; a replica may have no head yet. Restore checks the entries.
func (s *Store) read(pub ed25519.PublicKey, logID kololog.LogID, replica bool) (st stored, err error) {
	if st.header, err = s.b.Get(layout.HeaderKey(logID)); err != nil {
		if !errors.Is(err, ErrNotFound) {
			return st, err
		}
		if _, lerr := s.b.Get(layout.ListKey(logID)); lerr == nil {
			return st, corrupt(kololog.ErrMissingHeader)
		}
		return st, kololog.ErrMissingHeader
	}
	lh, err := kololog.DecodeLogHeader(st.header)
	if err != nil {
		return st, corrupt(err)
	}
	if lh.LogID != logID {
		return st, fmt.Errorf("%w: stored header belongs to log %s", ErrCorrupt, lh.LogID)
	}
	if err := kololog.VerifyLogHeader(pub, lh); err != nil {
		// A store holds one log per log ID: a valid header of another owner is that owner's log, not damage.
		if kololog.VerifyLogHeader(lh.LogKey, lh) == nil {
			return st, fmt.Errorf("%w: log %s is stored for another owner", kololog.ErrBadLogHeader, logID)
		}
		return st, corrupt(err)
	}
	kind, err := s.b.Get(layout.ListKey(logID))
	if errors.Is(err, ErrNotFound) {
		return st, fmt.Errorf("%w: log not listed", ErrCorrupt)
	}
	if err != nil {
		return st, err
	}
	switch isReplica, err := replicaKind(kind); {
	case err != nil:
		return st, err
	case isReplica && !replica:
		return st, ErrReplica
	case !isReplica && replica:
		return st, ErrNotReplica
	}
	st.head, err = s.b.Get(layout.HeadKey(logID))
	if errors.Is(err, ErrNotFound) && replica {
		st.head, err = nil, nil
	}
	if errors.Is(err, ErrNotFound) {
		return st, fmt.Errorf("%w: head missing", ErrCorrupt)
	}
	if err != nil {
		return st, err
	}
	if st.head != nil {
		h, err := kololog.DecodeHead(st.head)
		if err != nil {
			return st, corrupt(err)
		}
		if _, err := kololog.VerifyHead(pub, logID, h); err != nil {
			return st, corrupt(err)
		}
		st.heads.Head, st.headSize = &h, h.Size
	}
	size := st.headSize
	if replica {
		if size, err = s.readTarget(pub, logID, &st); err != nil {
			return st, err
		}
	}
	if err := s.checkGaps(logID, size, replica); err != nil {
		return st, err
	}
	st.size = size
	if f := beforeRestore.get(); f != nil {
		f()
	}
	return st, nil
}

// readTarget decodes and verifies a replica's unfinished catch-up and returns how many entries are stored.
func (s *Store) readTarget(pub ed25519.PublicKey, logID kololog.LogID, st *stored) (uint64, error) {
	v, err := s.b.Get(layout.TargetKey(logID))
	if errors.Is(err, ErrNotFound) {
		return st.headSize, nil
	}
	if err != nil {
		return 0, err
	}
	hb, pb, err := layout.DecodeTarget(v)
	if err != nil {
		return 0, corrupt(err)
	}
	h, err := kololog.DecodeHead(hb)
	if err != nil {
		return 0, corrupt(err)
	}
	if _, err := kololog.VerifyHead(pub, logID, h); err != nil {
		return 0, corrupt(err)
	}
	p, err := kololog.DecodeConsistencyProof(pb)
	if err != nil {
		return 0, corrupt(err)
	}
	if p.NewSize != h.Size || p.OldSize < st.headSize || p.OldSize >= h.Size {
		return 0, fmt.Errorf("%w: catch-up from %d to %d after a head of size %d", ErrCorrupt, p.OldSize, p.NewSize, st.headSize)
	}
	st.heads.Target, st.heads.Proof = &h, p
	return p.OldSize, nil
}

// replicaKind decodes a list value: empty for an own log, [layout.ReplicaKind] for a replica.
func replicaKind(v []byte) (bool, error) {
	switch {
	case len(v) == 0:
		return false, nil
	case bytes.Equal(v, layout.ReplicaKind):
		return true, nil
	}
	return false, fmt.Errorf("%w: bad list value %x", ErrCorrupt, v)
}

// checkGaps rejects any key of the log other than its header, head, a replica's target and the entries and keys below size,
// with bounded scans; the scans of Open check the keys below size.
func (s *Store) checkGaps(logID kololog.LogID, size uint64, replica bool) error {
	prefix := layout.LogPrefix(logID)
	entriesEnd := append(slices.Clone(prefix), layout.SuffixEntry+1)
	keysEnd := append(slices.Clone(prefix), layout.SuffixKey+1)
	var unexpected []byte
	stop := func(k, _ []byte) bool {
		unexpected = k
		return false
	}
	for _, r := range [][2][]byte{
		{prefix, layout.EntryKey(logID, 0)},
		{layout.EntryKey(logID, size), entriesEnd},
		{entriesEnd, layout.HeaderKey(logID)},
		{append(layout.HeaderKey(logID), 0), layout.KeyKey(logID, 0)},
		{layout.KeyKey(logID, size), keysEnd},
		{keysEnd, layout.HeadKey(logID)},
		{append(layout.HeadKey(logID), 0), layout.TargetKey(logID)},
		{layout.TargetKey(logID), append(layout.TargetKey(logID), 0)},
		{append(layout.TargetKey(logID), 0), layout.LogPrefixEnd(logID)},
	} {
		if replica && bytes.Equal(r[0], layout.TargetKey(logID)) {
			continue
		}
		if err := s.b.Scan(r[0], r[1], false, 1, stop); err != nil {
			return err
		}
		if unexpected != nil {
			return fmt.Errorf("%w: unexpected key %x", ErrCorrupt, unexpected)
		}
	}
	return nil
}

// Delete removes a closed log or replica, its header, head, entries and list key, in one atomic batch.
// Deleting a log that is not stored does nothing; a log open anywhere in this process returns [ErrLogInUse].
// The stored header is not trusted, so Delete holds a reservation of the log ID: deleting a replica also returns
// ErrLogInUse while the owner's writable log with that ID is open in this process.
func (s *Store) Delete(logID kololog.LogID) error {
	end, err := s.begin()
	if err != nil {
		return err
	}
	defer end()
	if err := s.reserve(logID); err != nil {
		return err
	}
	defer s.release(logID)
	unreserve, err := kololog.ReserveID(logID)
	if err != nil {
		return fmt.Errorf("%w: %s: %w", ErrLogInUse, logID, err)
	}
	defer unreserve()
	b := s.b.NewBatch()
	b.DeleteRange(layout.LogPrefix(logID), layout.LogPrefixEnd(logID))
	b.Delete(layout.ListKey(logID))
	if err := b.Commit(); err != nil {
		return fmt.Errorf("kolostore: delete log %s: %w", logID, err)
	}
	return nil
}

// List returns the IDs of the stored logs and replicas in byte order; [Store.Owner] tells them apart.
func (s *Store) List() ([]kololog.LogID, error) {
	end, err := s.begin()
	if err != nil {
		return nil, err
	}
	defer end()
	var ids []kololog.LogID
	var bad error
	err = s.b.Scan([]byte{layout.PrefixList}, []byte{layout.PrefixList + 1}, false, 0, func(k, v []byte) bool {
		if _, kindErr := replicaKind(v); len(k) != layout.LogPrefixLen || kindErr != nil {
			bad = fmt.Errorf("%w: bad list key %x", ErrCorrupt, k)
			return false
		}
		ids = append(ids, kololog.LogID(k[1:]))
		return true
	})
	if err == nil {
		err = bad
	}
	if err != nil {
		return nil, err
	}
	return ids, nil
}

// Owner returns the owner key named in a stored log's header and whether the log is a read-only replica, for example to reopen it after a restart.
// The key is checked only against the header's own signature; Open and OpenReplica verify everything against the key you pass them.
func (s *Store) Owner(logID kololog.LogID) (pub ed25519.PublicKey, replica bool, err error) {
	end, err := s.begin()
	if err != nil {
		return nil, false, err
	}
	defer end()
	kind, err := s.b.Get(layout.ListKey(logID))
	if err != nil {
		return nil, false, fmt.Errorf("kolostore: log %s: %w", logID, err)
	}
	if replica, err = replicaKind(kind); err != nil {
		return nil, false, err
	}
	header, err := s.b.Get(layout.HeaderKey(logID))
	if errors.Is(err, ErrNotFound) {
		return nil, false, fmt.Errorf("%w: header missing", ErrCorrupt)
	}
	if err != nil {
		return nil, false, err
	}
	h, err := kololog.DecodeLogHeader(header)
	if err != nil {
		return nil, false, corrupt(err)
	}
	if h.LogID != logID || kololog.VerifyLogHeader(h.LogKey, h) != nil {
		return nil, false, fmt.Errorf("%w: stored header does not belong to log %s", ErrCorrupt, logID)
	}
	return h.LogKey, replica, nil
}

// Close waits for running writes, closes every open Log and Replica of the store, then closes the backend.
// Every Close waits for the first one to finish and returns its error, also when the backend's Close panicked.
func (s *Store) Close() error {
	s.closeOnce.Do(func() { s.closeErr = s.shutdown() })
	return s.closeErr
}

func (s *Store) shutdown() error {
	s.mu.Lock()
	s.closed = true
	s.mu.Unlock()
	s.logsMu.Lock()
	logs := make([]handle, 0, len(s.logs))
	for l := range s.logs {
		logs = append(logs, l)
	}
	s.logsMu.Unlock()
	var errs []error
	for _, l := range logs {
		errs = append(errs, guard(fmt.Sprintf("closing log %s", l.ID()), l.Close))
	}
	if s.b != nil {
		errs = append(errs, guard("backend Close", s.b.Close))
	}
	return errors.Join(errs...)
}

// guard runs fn and turns a panic into an error, so one failure never stops the rest of the shutdown.
func guard(what string, fn func() error) (err error) {
	defer func() {
		if r := recover(); r != nil {
			if e, ok := r.(error); ok {
				err = fmt.Errorf("kolostore: %s panicked: %w", what, e)
			} else {
				err = fmt.Errorf("kolostore: %s panicked: %v", what, r)
			}
		}
	}()
	return fn()
}

func (s *Store) track(h handle) {
	s.logsMu.Lock()
	defer s.logsMu.Unlock()
	s.logs[h] = true
}

func (s *Store) untrack(h handle) {
	s.release(h.ID())
	s.logsMu.Lock()
	delete(s.logs, h)
	s.logsMu.Unlock()
}

// Log is an open log of a [Store]; it is safe for concurrent use and is the only way to write to the log.
// Every change is one atomic batch that kolo-log commits before it shows the change, so a failed write changes nothing.
type Log struct {
	s   *Store
	id  kololog.LogID
	log *kololog.Log

	mu     sync.RWMutex
	closed bool
}

func (l *Log) check() error {
	if l.closed {
		return ErrClosed
	}
	return nil
}

// Append appends v and durably stores the entry, its key and the new head in one batch, then returns its index.
// If the write fails, the log is unchanged and returns the error.
func (l *Log) Append(v any) (uint64, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return 0, err
	}
	return l.log.Append(v)
}

// Delete replaces the entries at indexes with delete markers signed by the log key and removes their keys, in one synced batch.
// An index out of range deletes nothing and deleted entries are skipped; if the write fails, every entry stays as it was.
// A backup, snapshot, write-ahead log or freed block from before keeps a working key, and after a reopen an old entry
// value put back reads as live.
func (l *Log) Delete(indexes ...uint64) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return err
	}
	return l.log.Delete(indexes...)
}

// Read decodes the entry at index into out.
func (l *Log) Read(index uint64, out any) error {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return err
	}
	return fromStorage(l.log.Read(index, out))
}

// Get returns the stored bytes of the entry at index.
func (l *Log) Get(index uint64) ([]byte, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return nil, err
	}
	v, err := l.log.Get(index)
	return v, fromStorage(err)
}

// Entry returns the stored form of the entry at index, its key included; clear Key when done with it.
func (l *Log) Entry(index uint64) (kololog.Entry, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return kololog.Entry{}, err
	}
	e, err := l.log.Entry(index)
	return e, fromStorage(err)
}

// Key returns the key of the entry at index without reading its ciphertext; clear it when done.
func (l *Log) Key(index uint64) (kololog.EntryKey, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return nil, err
	}
	k, err := l.log.Key(index)
	return k, fromStorage(err)
}

// EntryInfo returns whether the entry at index is deleted and whether its key is stored, without reading the backend.
func (l *Log) EntryInfo(index uint64) (kololog.EntryInfo, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return kololog.EntryInfo{}, err
	}
	return l.log.EntryInfo(index)
}

// Head returns a signed head for the stored tree.
func (l *Log) Head() (kololog.Head, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return kololog.Head{}, err
	}
	return l.log.Head()
}

// Header returns a copy of the signed log header; it also works after Close.
func (l *Log) Header() kololog.LogHeader {
	return l.log.Header()
}

// ID returns the log ID; it also works after Close.
func (l *Log) ID() kololog.LogID {
	return l.id
}

// Len returns the number of stored entries, deleted ones included; after Close it returns the last length.
func (l *Log) Len() uint64 {
	return l.log.Len()
}

// Record returns the public record of the entry at index.
func (l *Log) Record(index uint64) (kololog.Record, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return kololog.Record{}, err
	}
	return l.log.Record(index)
}

// InclusionProof returns a proof that the entry at index is in the tree of the first size entries.
func (l *Log) InclusionProof(index, size uint64) (kololog.InclusionProof, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return kololog.InclusionProof{}, err
	}
	return l.log.InclusionProof(index, size)
}

// ConsistencyProof returns a proof that the tree of oldSize entries is a prefix of the tree of newSize entries.
func (l *Log) ConsistencyProof(oldSize, newSize uint64) (kololog.ConsistencyProof, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	if err := l.check(); err != nil {
		return kololog.ConsistencyProof{}, err
	}
	return l.log.ConsistencyProof(oldSize, newSize)
}

// Close releases the log so it can be opened again; a second Close returns nil.
func (l *Log) Close() error {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return nil
	}
	l.closed = true
	if f := logClosing.get(); f != nil {
		f(l)
	}
	err := l.log.Close()
	l.s.untrack(l)
	return err
}

func decodeEntry(b []byte) (kololog.Entry, error) {
	e, err := layout.DecodeEntry(b)
	if err != nil {
		return kololog.Entry{}, corrupt(err)
	}
	return e, nil
}

func corrupt(err error) error {
	return fmt.Errorf("%w: %w", ErrCorrupt, err)
}
