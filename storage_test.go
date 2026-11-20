package kolostore

import (
	"bytes"
	"errors"
	"sync/atomic"
	"testing"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// storedPair stores on s a log of testIdentity with entries "a", "b", "c" and a replica of another owner's log with
// entries "x", "y", "z", leaves both closed, and returns them with the replica owner's key and their log IDs.
type storedPair struct {
	own, rep kololog.LogID
	owner    []byte
	writer   *Log
}

func newStoredPair(t testing.TB, s *Store) storedPair {
	t.Helper()
	l := mustCreate(t, s, testIdentity(t))
	for _, v := range []string{"a", "b", "c"} {
		mustAppend(t, l, []byte(v))
	}
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	for _, v := range []string{"x", "y", "z"} {
		mustAppend(t, w, []byte(v))
	}
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	if err := syncFrom(t, r, w); err != nil {
		t.Fatal(err)
	}
	l.Close()
	r.Close()
	return storedPair{own: l.ID(), rep: r.ID(), owner: owner.PublicKey(), writer: w}
}

func TestOpenStrayKeyAmongEntries(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		for _, at := range []uint64{0, 2} { // between two entries, and after the last one
			b := st.open(t)
			s := New(b)
			p := newStoredPair(t, s)
			for _, id := range []kololog.LogID{p.own, p.rep} {
				putRaw(t, b, append(layout.EntryKey(id, at), 0), []byte("x"))
			}
			if _, err := s.Open(testIdentity(t), p.own); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open with a stray key after entry %d: %v", at, err)
			}
			if _, err := s.OpenReplica(p.owner, p.rep, nil); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("OpenReplica with a stray key after entry %d: %v", at, err)
			}
			for _, id := range []kololog.LogID{p.own, p.rep} {
				delRaw(t, b, append(layout.EntryKey(id, at), 0))
			}
			l, err := s.Open(testIdentity(t), p.own)
			if err != nil {
				t.Fatalf("Open once the stray key is gone: %v", err)
			}
			l.Close()
			s.Close()
		}

		// An entry moved to a key that sorts in its place is still refused.
		b := st.open(t)
		s := New(b)
		defer s.Close()
		p := newStoredPair(t, s)
		for _, id := range []kololog.LogID{p.own, p.rep} {
			v := getRaw(t, b, layout.EntryKey(id, 1))
			delRaw(t, b, layout.EntryKey(id, 1))
			putRaw(t, b, append(layout.EntryKey(id, 0), 0), v)
		}
		if _, err := s.Open(testIdentity(t), p.own); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open with entry 1 under another key: %v", err)
		}
		if _, err := s.OpenReplica(p.owner, p.rep, nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("OpenReplica with entry 1 under another key: %v", err)
		}
	})
}

// reader is what a Log and a Replica of the store share for reading entries.
type reader interface {
	Get(index uint64) ([]byte, error)
	Read(index uint64, out any) error
	Entry(index uint64) (kololog.Entry, error)
}

func checkReadsCorrupt(t *testing.T, name string, r reader, index uint64, also error) {
	t.Helper()
	var out []byte
	_, gerr := r.Get(index)
	rerr := r.Read(index, &out)
	_, eerr := r.Entry(index)
	for what, err := range map[string]error{"Get": gerr, "Read": rerr, "Entry": eerr} {
		// Damage never looks like an index past the end.
		if !errors.Is(err, ErrCorrupt) || (also != nil && !errors.Is(err, also)) || errors.Is(err, kololog.ErrOutOfRange) {
			t.Errorf("%s: %s = %v, want ErrCorrupt and %v", name, what, err, also)
		}
	}
}

// After Open, every read goes to the backend and is checked against the entry's record, so a later change on disk is caught.
func TestReadsCheckStorage(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		s := openStore(t, b)
		p := newStoredPair(t, s)
		l, err := s.Open(testIdentity(t), p.own)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		r := mustOpenReplica(t, s, p.owner, p.rep, nil)
		defer r.Close()

		for _, c := range []struct {
			id kololog.LogID
			rd reader
		}{{p.own, l}, {p.rep, r}} {
			good := getRaw(t, b, layout.EntryKey(c.id, 1))
			e := storedEntry(t, b, c.id, 1)
			flipLast(e.Ciphertext)
			putStored(t, b, c.id, 1, e)
			checkReadsCorrupt(t, "tampered ciphertext", c.rd, 1, kololog.ErrStorageMismatch)
			putRaw(t, b, layout.EntryKey(c.id, 1), getRaw(t, b, layout.EntryKey(c.id, 0)))
			checkReadsCorrupt(t, "entry 0 stored as entry 1", c.rd, 1, kololog.ErrStorageMismatch)
			putRaw(t, b, layout.EntryKey(c.id, 1), good[:len(good)-1])
			checkReadsCorrupt(t, "value cut", c.rd, 1, nil)
			delRaw(t, b, layout.EntryKey(c.id, 1))
			checkReadsCorrupt(t, "entry removed", c.rd, 1, kololog.ErrStorageMismatch)
			putRaw(t, b, layout.EntryKey(c.id, 1), good)
			if got, err := c.rd.Get(1); err != nil || len(got) != 1 {
				t.Fatalf("Get once the entry is back: %q, %v", got, err)
			}
		}

		e := storedEntry(t, b, p.own, 2)
		e.Key = nil
		putStored(t, b, p.own, 2, e)
		checkReadsCorrupt(t, "own live entry without its key", l, 2, nil)

		key := storedEntry(t, b, p.rep, 2).Key
		delRaw(t, b, layout.KeyKey(p.rep, 2))
		checkReadsCorrupt(t, "replica key removed", r, 2, kololog.ErrStorageMismatch)
		if _, err := r.Key(2); !errors.Is(err, ErrCorrupt) || !errors.Is(err, kololog.ErrStorageMismatch) {
			t.Fatalf("Key removed from storage: %v", err)
		}
		if err := r.AddKey(9, key); !errors.Is(err, kololog.ErrOutOfRange) {
			t.Fatalf("AddKey past the end: %v", err)
		}
	})
}

// Delete and ApplyDelete take the entry's ciphertext and key out of storage, so the open log and a reopened one read ErrDeleted.
func TestDeleteRemovesStoredBytes(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		s := openStore(t, b)
		p := newStoredPair(t, s)
		l, err := s.Open(testIdentity(t), p.own)
		if err != nil {
			t.Fatal(err)
		}
		r := mustOpenReplica(t, s, p.owner, p.rep, nil)
		ciphertexts := map[kololog.LogID][]byte{p.own: storedEntry(t, b, p.own, 1).Ciphertext, p.rep: storedEntry(t, b, p.rep, 1).Ciphertext}

		if err := l.Delete(1); err != nil {
			t.Fatal(err)
		}
		if err := p.writer.Delete(1); err != nil {
			t.Fatal(err)
		}
		m, err := p.writer.Entry(1)
		if err != nil {
			t.Fatal(err)
		}
		if err := r.ApplyDelete(m.Marker); err != nil {
			t.Fatal(err)
		}
		for _, id := range []kololog.LogID{p.own, p.rep} {
			e := storedEntry(t, b, id, 1)
			if e.Ciphertext != nil || e.Key != nil || e.Marker == nil {
				t.Fatal("the stored entry kept its ciphertext or key")
			}
			for k, v := range snapshot(t, b) {
				if bytes.Contains([]byte(v), ciphertexts[id]) {
					t.Fatalf("the deleted ciphertext is still stored under %x", k)
				}
			}
		}
		for _, rd := range []reader{l, r} {
			if _, err := rd.Get(1); !errors.Is(err, kololog.ErrDeleted) {
				t.Fatalf("Get after the delete: %v", err)
			}
		}
		l.Close()
		r.Close()
		if l, err = s.Open(testIdentity(t), p.own); err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		r = mustOpenReplica(t, s, p.owner, p.rep, nil)
		defer r.Close()
		for _, rd := range []reader{l, r} {
			if _, err := rd.Get(1); !errors.Is(err, kololog.ErrDeleted) {
				t.Fatalf("Get after reopening: %v", err)
			}
			if got, err := rd.Get(2); err != nil || len(got) != 1 {
				t.Fatalf("Get(2) after reopening: %q, %v", got, err)
			}
		}
	})
}

// scanFailing fails every scan of the entries of a log, as a read error of the disk would.
type scanFailing struct {
	Backend
	on *atomic.Bool
}

func (f scanFailing) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	if f.on.Load() && limit == 0 && len(start) == layout.EntryKeyLen {
		return errInjected
	}
	return f.Backend.Scan(start, end, reverse, limit, fn)
}

func TestOpenScanError(t *testing.T) {
	on := new(atomic.Bool)
	s := openStore(t, scanFailing{NewMemory(), on})
	p := newStoredPair(t, s)
	on.Store(true)
	if _, err := s.Open(testIdentity(t), p.own); !errors.Is(err, errInjected) {
		t.Fatalf("Open with a failing scan: %v", err)
	}
	if _, err := s.OpenReplica(p.owner, p.rep, nil); !errors.Is(err, errInjected) {
		t.Fatalf("OpenReplica with a failing scan: %v", err)
	}
	on.Store(false)
	l, err := s.Open(testIdentity(t), p.own)
	if err != nil {
		t.Fatalf("Open after the scan works again: %v", err)
	}
	l.Close()
}

func TestSourceScanInOrder(t *testing.T) {
	s := openStore(t, NewMemory())
	p := newStoredPair(t, s)
	src := &logStorage{s: s, id: p.own}
	src.startScan(3)
	defer src.stopScan()
	if _, err := src.Entry(0); err != nil {
		t.Fatal(err)
	}
	if _, err := src.Entry(2); err == nil || errors.Is(err, ErrCorrupt) {
		t.Fatalf("Entry out of order during a scan: %v, want a non-corrupt error", err)
	}
}

func TestReplicaAddKeyWriteFails(t *testing.T) {
	fail := new(atomic.Bool)
	s := openStore(t, failing{NewMemory(), fail})
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	mustAppend(t, w, []byte("a"))
	e, err := w.Entry(0)
	if err != nil {
		t.Fatal(err)
	}
	key := bytes.Clone(e.Key)
	e.Key = nil
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	defer r.Close()
	if err := r.Update(mustHead(t, w), kololog.ConsistencyProof{}, []kololog.Entry{e}); err != nil {
		t.Fatal(err)
	}
	fail.Store(true)
	if err := r.AddKey(0, key); !errors.Is(err, errInjected) {
		t.Fatalf("AddKey with a failing write: %v", err)
	}
	fail.Store(false)
	if _, err := r.Get(0); !errors.Is(err, kololog.ErrKeyUnavailable) {
		t.Fatalf("Get after a failed AddKey: %v", err)
	}
	b := r.s.b
	good := storedEntry(t, b, w.ID(), 0)
	bad := good
	bad.Ciphertext = flip(good.Ciphertext)
	putStored(t, b, w.ID(), 0, bad)
	if err := r.AddKey(0, key); !errors.Is(err, ErrCorrupt) || !errors.Is(err, kololog.ErrStorageMismatch) {
		t.Fatalf("AddKey over a changed stored entry: %v", err)
	}
	putStored(t, b, w.ID(), 0, good)
	if err := r.AddKey(0, key); err != nil {
		t.Fatal(err)
	}
	if got, err := r.Get(0); err != nil || string(got) != "a" {
		t.Fatalf("Get after AddKey: %q, %v", got, err)
	}
}

func flip(b []byte) []byte {
	b = bytes.Clone(b)
	b[len(b)-1] ^= 1
	return b
}

// Keys and entries out of place in the stored ranges are corrupt for an own log and a replica.
func TestOpenStrayKeysInRanges(t *testing.T) {
	cases := map[string]func(Backend, kololog.LogID){
		"key renamed to a malformed key": func(b Backend, id kololog.LogID) {
			putRaw(t, b, append(layout.KeyKey(id, 0), 0), getRaw(t, b, layout.KeyKey(id, 1)))
			delRaw(t, b, layout.KeyKey(id, 1))
		},
		"stray key after the last key": func(b Backend, id kololog.LogID) {
			putRaw(t, b, append(layout.KeyKey(id, 2), 0), []byte("x"))
		},
		"stray entry after the last entry": func(b Backend, id kololog.LogID) {
			putRaw(t, b, append(layout.EntryKey(id, 2), 0), []byte("x"))
		},
	}
	for name, change := range cases {
		b := NewMemory()
		s := New(keepOpen{b})
		p := newStoredPair(t, s)
		for _, id := range []kololog.LogID{p.own, p.rep} {
			change(b, id)
		}
		if _, err := s.Open(testIdentity(t), p.own); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: Open = %v, want ErrCorrupt", name, err)
		}
		if _, err := s.OpenReplica(p.owner, p.rep, nil); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: OpenReplica = %v, want ErrCorrupt", name, err)
		}
		s.Close()
	}
}

// Deleting many entries of a log, or applying many markers to a replica, is one batch, and a failed batch changes nothing on disk or in memory.
func TestDeleteManyOneBatch(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		fail := new(atomic.Bool)
		writes := 0
		b := st.open(t)
		s := openStore(t, countingBatches{failing{b, fail}, &writes})
		id := testIdentity(t)
		l := mustCreate(t, s, id)
		for i := range 20 {
			mustAppend(t, l, []byte{byte(i)})
		}
		owner := newIdentity(t)
		w := mustCreate(t, openStore(t, NewMemory()), owner)
		for i := range 20 {
			mustAppend(t, w, []byte{byte(i)})
		}
		r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
		if err := syncFrom(t, r, w); err != nil {
			t.Fatal(err)
		}
		all := make([]uint64, 20)
		for i := range all {
			all[i] = uint64(i)
		}
		if err := w.Delete(all...); err != nil {
			t.Fatal(err)
		}
		var markers [][]byte
		for i := range uint64(20) {
			e, err := w.Entry(i)
			if err != nil {
				t.Fatal(err)
			}
			markers = append(markers, e.Marker)
		}

		before := snapshot(t, b)
		fail.Store(true)
		if err := l.Delete(all...); !errors.Is(err, errInjected) {
			t.Fatalf("Delete with a failing batch: %v", err)
		}
		if err := r.ApplyDelete(markers...); !errors.Is(err, errInjected) {
			t.Fatalf("ApplyDelete with a failing batch: %v", err)
		}
		fail.Store(false)
		if !sameSnapshot(before, snapshot(t, b)) {
			t.Fatal("a failed batch changed the disk")
		}
		for _, rd := range []reader{l, r} {
			if got, err := rd.Get(7); err != nil || len(got) != 1 || got[0] != 7 {
				t.Fatalf("Get after a failed batch: %v, %v", got, err)
			}
		}

		writes = 0
		if err := l.Delete(all...); err != nil {
			t.Fatal(err)
		}
		if err := r.ApplyDelete(markers...); err != nil {
			t.Fatal(err)
		}
		if writes != 2 {
			t.Fatalf("deleting 20 entries of a log and of a replica made %d batches, want 2", writes)
		}
		l.Close()
		r.Close()
		l, err := s.Open(id, l.ID())
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
		defer r.Close()
		for i := range uint64(20) {
			for _, rd := range []reader{l, r} {
				if _, err := rd.Get(i); !errors.Is(err, kololog.ErrDeleted) {
					t.Fatalf("Get(%d) after reopening: %v", i, err)
				}
			}
			if k, err := b.Get(layout.KeyKey(l.ID(), i)); !errors.Is(err, ErrNotFound) {
				t.Fatalf("key of deleted entry %d still stored: %x, %v", i, k, err)
			}
		}
	})
}

// headerFor returns a log header for logID signed by id, as another owner could make for an ID it saw.
func headerFor(t testing.TB, id *koloid.Identity, logID kololog.LogID) []byte {
	t.Helper()
	var e kolocodec.Encoder
	e.PutString(kololog.HeaderContext)
	e.PutFixed(id.PublicKey())
	e.PutFixed(logID[:])
	e.PutUvarint(uint64(kololog.Bytes))
	msg := e.Bytes()
	sig, err := id.Sign(kololog.HeaderContext, msg)
	if err != nil {
		t.Fatal(err)
	}
	h, err := kololog.DecodeLogHeader(append(msg, sig...))
	if err != nil || kololog.VerifyLogHeader(id.PublicKey(), h) != nil {
		t.Fatalf("test header does not verify: %v", err)
	}
	return h.Encode()
}

// A store holds one log per log ID: another owner's log or replica under an ID already stored is refused with ErrBadLogHeader,
// is never returned for the other owner and leaves the disk unchanged.
func TestOtherOwnerSameLogID(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		s := openStore(t, b)
		p := newStoredPair(t, s) // p.own belongs to testIdentity, p.rep to p.owner
		other := newIdentity(t)
		before := snapshot(t, b)
		for _, id := range []kololog.LogID{p.own, p.rep} {
			for name, header := range map[string][]byte{"no header": nil, "its own header": headerFor(t, other, id)} {
				r, err := s.OpenReplica(other.PublicKey(), id, header)
				if r != nil || !errors.Is(err, kololog.ErrBadLogHeader) || errors.Is(err, ErrCorrupt) {
					t.Fatalf("OpenReplica by another owner of a stored ID, %s: %v, %v", name, r, err)
				}
			}
			l, err := s.Open(other, id)
			if l != nil || !errors.Is(err, kololog.ErrBadLogHeader) || errors.Is(err, ErrCorrupt) {
				t.Fatalf("Open by another owner of a stored ID: %v, %v", l, err)
			}
		}
		if !sameSnapshot(before, snapshot(t, b)) {
			t.Fatal("refused opens changed the disk")
		}
		// The stored log and replica still open for their owners, and an open one is refused to anyone else.
		l, err := s.Open(testIdentity(t), p.own)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		if _, err := s.OpenReplica(other.PublicKey(), p.own, headerFor(t, other, p.own)); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("OpenReplica of an ID open in the store: %v", err)
		}
		if got, err := l.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("the owner's open log after the refusals: %q, %v", got, err)
		}
		r := mustOpenReplica(t, s, p.owner, p.rep, nil)
		defer r.Close()
		if got, err := r.Get(0); err != nil || string(got) != "x" {
			t.Fatalf("the owner's replica after the refusals: %q, %v", got, err)
		}
		if !sameSnapshot(before, snapshot(t, b)) {
			t.Fatal("opening for the owners changed the disk")
		}
	})
}

// failingEntryGets fails every Get of an entry key, as a read error of the disk would.
type failingEntryGets struct{ Backend }

func (f failingEntryGets) Get(key []byte) ([]byte, error) {
	if len(key) == layout.EntryKeyLen {
		return nil, errInjected
	}
	return f.Backend.Get(key)
}

// Create and OpenReplica check that the new log's storage is empty, and a failed read refuses them instead of passing for empty.
func TestCreateChecksStorageReadFails(t *testing.T) {
	s := openStore(t, failingEntryGets{NewMemory()})
	if _, err := s.Create(testIdentity(t)); !errors.Is(err, errInjected) {
		t.Fatalf("Create with failing entry reads: %v", err)
	}
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	if _, err := s.OpenReplica(owner.PublicKey(), w.ID(), w.Header().Encode()); !errors.Is(err, errInjected) {
		t.Fatalf("OpenReplica of a new replica with failing entry reads: %v", err)
	}
	if ids, err := s.List(); err != nil || len(ids) != 0 {
		t.Fatalf("refused creates stored %v, %v", ids, err)
	}
	st := &logStorage{s: openStore(t, NewMemory()), id: w.ID()}
	if _, err := st.Entry(0); !errors.Is(err, kololog.ErrOutOfRange) || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("a missing entry: %v, want ErrOutOfRange and ErrCorrupt", err)
	}
}
