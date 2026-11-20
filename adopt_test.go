package kolostore

import (
	"bytes"
	"errors"
	"maps"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

var utf8 = kololog.WithValueEncoding(kololog.UTF8)

// adoptCase is a device that stored a log with 3 entries on a backend of st, while the writer went on to 5 entries and deleted entry 1.
// The writer's log is closed, and rep is a full replica of it in a store of its own.
type adoptCase struct {
	st      storage
	id      *koloid.Identity
	logID   kololog.LogID
	header  []byte
	sw      *Store
	wb      Backend // the writer's backend, whose keys the adopted log must match
	at0     Backend // a copy of the writer's backend with the empty log
	at2     Backend // a copy of the writer's backend at 2 entries
	rep     *Replica
	old     map[string]string // the stored backend before Adopt
	proof35 kololog.ConsistencyProof
	proof45 kololog.ConsistencyProof
	heads   map[uint64]kololog.Head
	entries map[uint64][]kololog.Entry
	live5   []kololog.Entry // the 5 entries before entry 1 was deleted
}

func newAdoptCase(t testing.TB, st storage) *adoptCase {
	t.Helper()
	c := &adoptCase{st: st, id: testIdentity(t), wb: NewMemory(), heads: map[uint64]kololog.Head{}, entries: map[uint64][]kololog.Entry{}}
	c.sw = openStore(t, c.wb)
	w := mustCreate(t, c.sw, c.id, utf8)
	c.logID, c.header = w.ID(), w.Header().Encode()
	c.at0 = copyBackend(t, c.wb)
	grab := func() {
		n := w.Len()
		c.heads[n] = mustHead(t, w)
		for i := range n {
			e, err := w.Entry(i)
			if err != nil {
				t.Fatal(err)
			}
			c.entries[n] = append(c.entries[n], e)
		}
	}
	for i, v := range []string{"a", "b", "c", "d", "e"} {
		mustAppend(t, w, v)
		if i == 1 || i == 2 {
			grab()
		}
		if i == 1 {
			c.at2 = copyBackend(t, c.wb)
		}
		if i == 2 {
			b := st.open(t)
			for k, v := range snapshot(t, c.wb) {
				putRaw(t, b, []byte(k), []byte(v))
			}
			c.old = snapshot(t, b)
			if err := b.Close(); err != nil {
				t.Fatal(err)
			}
		}
	}
	var err error
	if c.proof35, err = w.ConsistencyProof(3, 5); err != nil {
		t.Fatal(err)
	}
	if c.proof45, err = w.ConsistencyProof(4, 5); err != nil {
		t.Fatal(err)
	}
	grab()
	c.live5, c.entries[5] = c.entries[5], nil
	if err := w.Delete(1); err != nil {
		t.Fatal(err)
	}
	grab()
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	c.rep = c.replica(t, 5)
	return c
}

// replica returns a new replica of the writer's log with its first n entries, n being 2, 3 or 5.
func (c *adoptCase) replica(t testing.TB, n uint64) *Replica {
	t.Helper()
	return c.replicaOf(t, c.header, c.heads[n], c.entries[n])
}

func (c *adoptCase) replicaOf(t testing.TB, header []byte, h kololog.Head, entries []kololog.Entry) *Replica {
	t.Helper()
	r := mustOpenReplica(t, openStore(t, NewMemory()), c.id.PublicKey(), c.logID, header)
	if err := r.Update(h, kololog.ConsistencyProof{}, entries); err != nil {
		t.Fatal(err)
	}
	return r
}

func copyBackend(t testing.TB, from Backend) Backend {
	t.Helper()
	b := NewMemory()
	for k, v := range snapshot(t, from) {
		putRaw(t, b, []byte(k), []byte(v))
	}
	return b
}

// unchanged fails when the backend differs from the stored state before Adopt.
func (c *adoptCase) unchanged(t *testing.T, b Backend) {
	t.Helper()
	if !maps.Equal(snapshot(t, b), c.old) {
		t.Fatal("a refused Adopt changed the store")
	}
}

func TestAdopt(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		b := st.open(t)
		s := openStore(t, b)
		other := newStoredPair(t, s)
		before := snapshot(t, b)

		l, err := s.Adopt(c.id, c.rep, utf8)
		if err != nil {
			t.Fatal(err)
		}
		if l.ID() != c.logID || l.Len() != 5 || !sameTree(mustHead(t, l), c.heads[5]) {
			t.Fatalf("adopted log has ID %s, Len %d", l.ID(), l.Len())
		}
		after := snapshot(t, b)
		for k, v := range before {
			if !strings.HasPrefix(k, string(layout.LogPrefix(c.logID))) && after[k] != v {
				t.Fatalf("Adopt changed key %x of another log", k)
			}
		}
		for k, v := range snapshot(t, c.wb) {
			if after[k] != v {
				t.Fatalf("adopted log differs from the writer's at key %x", k)
			}
		}
		for k := range after {
			if _, ok := before[k]; !ok && !strings.HasPrefix(k, string(layout.LogPrefix(c.logID))) {
				t.Fatalf("Adopt added key %x outside the log", k)
			}
		}
		var text string
		for i, want := range map[uint64]string{0: "a", 2: "c", 4: "e"} {
			if err := l.Read(i, &text); err != nil || text != want {
				t.Fatalf("Read(%d) = %q, %v, want %q", i, text, err, want)
			}
		}
		if _, err := l.Get(1); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("Get(1) of the deleted entry: %v", err)
		}
		if got := mustAppend(t, l, "f"); got != 5 {
			t.Fatalf("Append after Adopt returned %d, want 5", got)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		l, err = s.Open(c.id, c.logID, utf8)
		if err != nil {
			t.Fatalf("Open after Adopt: %v", err)
		}
		defer l.Close()
		if err := l.Read(5, &text); err != nil || text != "f" || l.Len() != 6 {
			t.Fatalf("reopened log: Read(5) = %q, %v, Len %d", text, err, l.Len())
		}
		if ids, err := s.List(); err != nil || len(ids) != 3 {
			t.Fatalf("List = %v, %v, want the adopted log and the two others", ids, err)
		}
		if l2, err := s.Open(c.id, other.own); err != nil {
			t.Fatalf("Open of another log: %v", err)
		} else {
			l2.Close()
		}
	})
}

func TestAdoptEqualSizeAndEncoding(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	s := openStore(t, c.st.open(t))
	l, err := s.Adopt(c.id, c.replica(t, 3), utf8)
	if err != nil {
		t.Fatalf("Adopt of a replica of the stored size: %v", err)
	}
	if l.Len() != 3 || !sameTree(mustHead(t, l), c.heads[3]) {
		t.Fatalf("adopted log has Len %d", l.Len())
	}
	l.Close()

	// Without the encoding option the batch is committed and only the open fails, so Open with the option works.
	c = newAdoptCase(t, kinds[0].new(t))
	s = openStore(t, c.st.open(t))
	if _, err := s.Adopt(c.id, c.rep); !errors.Is(err, kololog.ErrEncodingMismatch) {
		t.Fatalf("Adopt without the encoding option: %v", err)
	}
	l, err = s.Open(c.id, c.logID, utf8)
	if err != nil || l.Len() != 5 {
		t.Fatalf("Open after a failed open of Adopt: %v", err)
	}
	l.Close()
}

// forgedReplica is a replica, with no head, of a log by attacker that claims logID.
func forgedReplica(t *testing.T, attacker *koloid.Identity, logID kololog.LogID) *Replica {
	t.Helper()
	h := kololog.LogHeader{LogKey: attacker.PublicKey(), LogID: logID, Encoding: kololog.UTF8}
	sig, err := attacker.Sign(kololog.HeaderContext, h.Encode())
	if err != nil {
		t.Fatal(err)
	}
	h.Signature = sig
	return mustOpenReplica(t, openStore(t, NewMemory()), attacker.PublicKey(), logID, h.Encode())
}

func TestAdoptRefusals(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		b := st.open(t)
		s := openStore(t, b)
		refuse := func(name string, want error, id *koloid.Identity, rep *Replica) {
			t.Helper()
			l, err := s.Adopt(id, rep, utf8)
			if l != nil {
				l.Close()
			}
			if !errors.Is(err, want) {
				t.Fatalf("%s: Adopt = %v, want %v", name, err, want)
			}
			c.unchanged(t, b)
		}

		attacker := newIdentity(t)
		refuse("another identity", kololog.ErrBadLogHeader, attacker, c.rep)
		refuse("a replica of another owner with the log ID", kololog.ErrBadLogHeader, c.id, forgedReplica(t, attacker, c.logID))

		w2 := mustCreate(t, openStore(t, NewMemory()), c.id, utf8)
		mustAppend(t, w2, "x")
		r2 := mustOpenReplica(t, openStore(t, NewMemory()), c.id.PublicKey(), w2.ID(), w2.Header().Encode())
		if err := syncFrom(t, r2, w2); err != nil {
			t.Fatal(err)
		}
		w2.Close()
		refuse("another log ID", kololog.ErrMissingHeader, c.id, r2)

		refuse("no head", ErrIncomplete, c.id, mustOpenReplica(t, openStore(t, NewMemory()), c.id.PublicKey(), c.logID, c.header))
		part := c.replica(t, 3)
		if err := part.Update(c.heads[5], c.proof45, c.entries[5][3:4]); err != nil {
			t.Fatal(err)
		}
		if _, _, ok := part.Target(); !ok {
			t.Fatal("the chunk left no pending target")
		}
		refuse("pending catch-up", ErrIncomplete, c.id, part)
		empty := c.replica(t, 3)
		if err := empty.Update(c.heads[5], c.proof35, nil); err != nil {
			t.Fatal(err)
		}
		if _, _, ok := empty.Target(); !ok || empty.Len() != c.heads[3].Size {
			t.Fatalf("the empty chunk left Len %d and target %v", empty.Len(), ok)
		}
		refuse("pending catch-up with Len at the head", ErrIncomplete, c.id, empty)

		refuse("a shorter replica", kololog.ErrRollback, c.id, c.replica(t, 2))

		withoutKey := slices.Clone(c.entries[5])
		withoutKey[3].Key = nil
		keyless := c.replicaOf(t, c.header, c.heads[5], withoutKey)
		refuse("a live entry past the stored ones without its key", kololog.ErrKeyUnavailable, c.id, keyless)
		if err := keyless.AddKey(3, c.entries[5][3].Key); err != nil {
			t.Fatal(err)
		}
		if l, err := s.Adopt(c.id, keyless, utf8); err != nil {
			t.Fatalf("Adopt once the key is there: %v", err)
		} else {
			l.Close()
		}

		closed := c.replica(t, 5)
		closed.Close()
		if _, err := s.Adopt(c.id, closed); err == nil {
			t.Fatal("Adopt of a closed replica succeeded")
		}
		if _, err := s.Adopt(c.id, nil); err == nil {
			t.Fatal("Adopt of a nil replica succeeded")
		}
	})
}

func TestAdoptRefusesStoredReplicaAndMissingLog(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	b := NewMemory()
	s := openStore(t, b)
	if _, err := s.Adopt(c.id, c.rep, utf8); !errors.Is(err, kololog.ErrMissingHeader) {
		t.Fatalf("Adopt into an empty store: %v", err)
	}
	if _, err := s.Adopt(newIdentity(t), c.rep, utf8); !errors.Is(err, kololog.ErrBadLogHeader) {
		t.Fatalf("Adopt of another owner's replica into an empty store: %v", err)
	}
	if n := len(snapshot(t, b)); n != 0 {
		t.Fatalf("Adopt into an empty store wrote %d keys", n)
	}
	r := mustOpenReplica(t, s, c.id.PublicKey(), c.logID, c.header)
	r.Close()
	before := snapshot(t, b)
	if _, err := s.Adopt(c.id, c.rep, utf8); !errors.Is(err, ErrReplica) {
		t.Fatalf("Adopt over a stored replica: %v", err)
	}
	if !maps.Equal(snapshot(t, b), before) {
		t.Fatal("Adopt over a stored replica changed the store")
	}
}

func TestAdoptOpenLog(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		b := st.open(t)
		s := openStore(t, b)
		l, err := s.Open(c.id, c.logID, utf8)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Adopt(c.id, c.rep, utf8); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("Adopt over an open log: %v", err)
		}
		c.unchanged(t, b)
		if got := mustAppend(t, l, "x"); got != 3 {
			t.Fatalf("the open log appended at %d after the refused Adopt, want 3", got)
		}
		l.Close()

		// Another store on the same backend value shares the open logs.
		l, err = s.Open(c.id, c.logID, utf8)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		s2 := New(b)
		if _, err := s2.Adopt(c.id, c.rep, utf8); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("Adopt over a log open through another store: %v", err)
		}
	})
}

func TestAdoptOpenInOtherBackend(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	s := openStore(t, c.st.open(t))
	// The writer's log has this key and ID and is open in this process, so the restore cannot take it over.
	w, err := c.sw.Open(c.id, c.logID, utf8)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Adopt(c.id, c.rep, utf8); !errors.Is(err, ErrLogInUse) {
		t.Fatalf("Adopt while the writer's log with this ID is open: %v", err)
	}
	w.Close()
	l, err := s.Adopt(c.id, c.rep, utf8)
	if err != nil {
		t.Fatalf("Adopt once it is closed: %v", err)
	}
	l.Close()
}

func TestAdoptConcurrent(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	s := openStore(t, c.st.open(t))
	var wg sync.WaitGroup
	var won, busy atomic.Int32
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			l, err := s.Adopt(c.id, c.rep, utf8)
			switch {
			case err == nil:
				won.Add(1)
				defer l.Close()
			case errors.Is(err, ErrLogInUse):
				busy.Add(1)
			default:
				t.Errorf("Adopt: %v", err)
			}
		}()
	}
	wg.Wait()
	if won.Load() < 1 || won.Load()+busy.Load() != 8 {
		t.Fatalf("%d adopted, %d refused", won.Load(), busy.Load())
	}
}

// crashAt is a backend that dies at its at-th batch commit: the batch is dropped, or applied first when after is set,
// and every later read fails, like a process that ended there.
type crashAt struct {
	Backend
	at    int
	after bool
	n     *int
	dead  *atomic.Bool
}

func (c crashAt) Get(k []byte) ([]byte, error) {
	if c.dead.Load() {
		return nil, errInjected
	}
	return c.Backend.Get(k)
}

func (c crashAt) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	if c.dead.Load() {
		return errInjected
	}
	return c.Backend.Scan(start, end, reverse, limit, fn)
}

func (c crashAt) NewBatch() Batch { return crashBatch{c.Backend.NewBatch(), c} }

type crashBatch struct {
	Batch
	c crashAt
}

func (b crashBatch) Commit() error {
	*b.c.n++
	if *b.c.n != b.c.at {
		return b.Batch.Commit()
	}
	defer b.c.dead.Store(true)
	if b.c.after {
		return b.Batch.Commit()
	}
	return errInjected
}

// TestAdoptCrash crashes just before and just after the one batch of Adopt; the store then opens the old log or the new one.
func TestAdoptCrash(t *testing.T) {
	for _, k := range kinds {
		for _, tc := range []struct {
			name  string
			after bool
		}{
			{"before the batch", false},
			{"after the batch", true},
		} {
			t.Run(k.name+"/"+tc.name, func(t *testing.T) {
				st := k.new(t)
				c := newAdoptCase(t, st)
				var n int
				s := New(crashAt{st.open(t), 1, tc.after, &n, new(atomic.Bool)})
				l, err := s.Adopt(c.id, c.rep, utf8)
				if !errors.Is(err, errInjected) {
					t.Fatalf("Adopt = %v", err)
				}
				if l != nil {
					l.Close()
				}
				s.Close()

				s = openStore(t, st.open(t))
				l, err = s.Open(c.id, c.logID, utf8)
				if err != nil {
					t.Fatalf("Open after the crash: %v", err)
				}
				defer l.Close()
				want := uint64(3)
				if tc.after {
					want = 5
				}
				if l.Len() != want || mustAppend(t, l, "x") != want {
					t.Fatalf("the log has %d entries, want %d", l.Len()-1, want)
				}
				if ids, err := s.List(); err != nil || len(ids) != 1 {
					t.Fatalf("List = %v, %v", ids, err)
				}
			})
		}
	}
}

func TestAdoptOneBatch(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	var n int
	s := openStore(t, countingBatches{c.st.open(t), &n})
	l, err := s.Adopt(c.id, c.rep, utf8)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	if n != 1 {
		t.Fatalf("Adopt used %d batches, want 1", n)
	}
}

func TestAdoptClosedStore(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	s := New(c.st.open(t))
	s.Close()
	if _, err := s.Adopt(c.id, c.rep, utf8); !errors.Is(err, ErrClosed) {
		t.Fatalf("Adopt on a closed store: %v", err)
	}
	if _, err := New(nil).Adopt(c.id, c.rep, utf8); err == nil {
		t.Fatal("Adopt on a store without a backend succeeded")
	}
}

// TestAdoptRefusesFork adopts owner-signed second histories that split from the stored log at 2 entries, of equal and of greater size.
func TestAdoptRefusesFork(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		f, err := openStore(t, c.at2).Open(c.id, c.logID, utf8)
		if err != nil {
			t.Fatal(err)
		}
		mustAppend(t, f, "Z")
		fork3 := mustOpenReplica(t, openStore(t, NewMemory()), c.id.PublicKey(), c.logID, c.header)
		if err := syncFrom(t, fork3, f); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, f, "Y")
		mustAppend(t, f, "W")
		fork5 := mustOpenReplica(t, openStore(t, NewMemory()), c.id.PublicKey(), c.logID, c.header)
		if err := syncFrom(t, fork5, f); err != nil {
			t.Fatal(err)
		}
		f.Close()
		b := st.open(t)
		s := openStore(t, b)
		for name, r := range map[string]*Replica{"equal-size fork": fork3, "longer fork": fork5} {
			if l, err := s.Adopt(c.id, r, utf8); !errors.Is(err, kololog.ErrRollback) {
				if l != nil {
					l.Close()
				}
				t.Fatalf("%s: Adopt = %v, want ErrRollback", name, err)
			}
			c.unchanged(t, b)
		}
	})
}

// TestAdoptRefusesOtherHeader adopts a replica whose header the owner signed again for the stored log ID with another encoding.
func TestAdoptRefusesOtherHeader(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		h := kololog.LogHeader{LogKey: c.id.PublicKey(), LogID: c.logID, Encoding: kololog.Bytes}
		sig, err := c.id.Sign(kololog.HeaderContext, h.Encode())
		if err != nil {
			t.Fatal(err)
		}
		h.Signature = sig
		rep := c.replicaOf(t, h.Encode(), c.heads[5], c.entries[5])
		b := st.open(t)
		s := openStore(t, b)
		bytesEnc := kololog.WithValueEncoding(kololog.Bytes)
		if l, err := s.Adopt(c.id, rep, bytesEnc); !errors.Is(err, kololog.ErrBadLogHeader) {
			if l != nil {
				l.Close()
			}
			t.Fatalf("Adopt with another header: %v", err)
		}
		c.unchanged(t, b)
	})
}

// TestAdoptKeepsDeletions adopts a replica that deleted entry 1 into a store that deleted entry 2, which the replica still holds live
// with its key: both stay deleted, before and after a reopen, and neither key is stored.
func TestAdoptKeepsDeletions(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		b := st.open(t)
		s := openStore(t, b)
		d, err := s.Open(c.id, c.logID, utf8)
		if err != nil {
			t.Fatal(err)
		}
		if err := d.Delete(2); err != nil {
			t.Fatal(err)
		}
		d.Close()
		if info, err := c.rep.EntryInfo(2); err != nil || info != (kololog.EntryInfo{Keyed: true}) {
			t.Fatalf("replica entry 2: %+v, %v", info, err)
		}
		check := func(l *Log) {
			t.Helper()
			var text string
			for i := range uint64(5) {
				info, err := l.EntryInfo(i)
				deleted := i == 1 || i == 2
				if err != nil || info != (kololog.EntryInfo{Deleted: deleted, Keyed: !deleted}) {
					t.Fatalf("EntryInfo(%d) = %+v, %v", i, info, err)
				}
				if err := l.Read(i, &text); deleted != errors.Is(err, kololog.ErrDeleted) || (!deleted && err != nil) {
					t.Fatalf("Read(%d) = %q, %v", i, text, err)
				}
			}
			for _, i := range []uint64{1, 2} {
				if _, err := b.Get(layout.KeyKey(c.logID, i)); !errors.Is(err, ErrNotFound) {
					t.Fatalf("key of deleted entry %d: %v", i, err)
				}
			}
		}
		l, err := s.Adopt(c.id, c.rep, utf8)
		if err != nil {
			t.Fatal(err)
		}
		check(l)
		l.Close()
		if l, err = s.Open(c.id, c.logID, utf8); err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		check(l)
	})
}

// TestAdoptRefusesDamagedPrefix damages the stored entries Adopt keeps; each is refused with ErrCorrupt and nothing is written.
func TestAdoptRefusesDamagedPrefix(t *testing.T) {
	for _, tc := range []struct {
		name   string
		damage func(t *testing.T, b Backend, logID kololog.LogID)
	}{
		{"a bad delete marker", func(t *testing.T, b Backend, logID kololog.LogID) {
			v, err := b.Get(layout.EntryKey(logID, 2))
			if err != nil {
				t.Fatal(err)
			}
			v[len(v)-1] ^= 1
			putRaw(t, b, layout.EntryKey(logID, 2), v)
		}},
		{"the marker of another entry", func(t *testing.T, b Backend, logID kololog.LogID) {
			e1, err := b.Get(layout.EntryKey(logID, 0))
			if err != nil {
				t.Fatal(err)
			}
			e2, err := b.Get(layout.EntryKey(logID, 2))
			if err != nil {
				t.Fatal(err)
			}
			d1, _ := layout.DecodeEntry(e1)
			d2, _ := layout.DecodeEntry(e2)
			putRaw(t, b, layout.EntryKey(logID, 2), layout.EncodeEntry(kololog.Entry{Record: d1.Record, Marker: d2.Marker}))
		}},
		{"a live entry without its key", func(t *testing.T, b Backend, logID kololog.LogID) {
			if err := b.Delete(layout.KeyKey(logID, 0)); err != nil {
				t.Fatal(err)
			}
		}},
		{"an entry missing", func(t *testing.T, b Backend, logID kololog.LogID) {
			if err := b.Delete(layout.EntryKey(logID, 2)); err != nil {
				t.Fatal(err)
			}
		}},
		{"an entry that does not decode", func(t *testing.T, b Backend, logID kololog.LogID) {
			putRaw(t, b, layout.EntryKey(logID, 2), []byte{0xff})
		}},
		{"a stray key among the keys", func(t *testing.T, b Backend, logID kololog.LogID) {
			putRaw(t, b, append(layout.KeyKey(logID, 1), 0), make([]byte, layout.KeySize))
		}},
	} {
		forEachBackend(t, func(t *testing.T, st storage) {
			c := newAdoptCase(t, st)
			b := st.open(t)
			s := openStore(t, b)
			d, err := s.Open(c.id, c.logID, utf8)
			if err != nil {
				t.Fatal(err)
			}
			if err := d.Delete(2); err != nil {
				t.Fatal(err)
			}
			d.Close()
			tc.damage(t, b, c.logID)
			before := snapshot(t, b)
			rep := c.replicaOf(t, c.header, c.heads[5], c.live5)
			if l, err := s.Adopt(c.id, rep, utf8); !errors.Is(err, ErrCorrupt) {
				if l != nil {
					l.Close()
				}
				t.Fatalf("%s: Adopt = %v, want ErrCorrupt", tc.name, err)
			}
			if !maps.Equal(snapshot(t, b), before) {
				t.Fatalf("%s: the refused Adopt changed the store", tc.name)
			}
		})
	}
}

// TestAdoptKeylessPrefix adopts a replica that lacks the keys of entries the store already holds with their keys.
func TestAdoptKeylessPrefix(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	entries := slices.Clone(c.entries[5])
	for i := range 3 {
		entries[i].Key = nil
	}
	s := openStore(t, c.st.open(t))
	l, err := s.Adopt(c.id, c.replicaOf(t, c.header, c.heads[5], entries), utf8)
	if err != nil {
		t.Fatal(err)
	}
	defer l.Close()
	var text string
	if err := l.Read(2, &text); err != nil || text != "c" {
		t.Fatalf("Read(2) = %q, %v", text, err)
	}
}

// adoptCost is what the batch of Adopt of c.rep counts: entry 1 deleted, entries 3 and 4 with their keys, and the head.
func adoptCost(t *testing.T, c *adoptCase) uint64 {
	t.Helper()
	w := snapshot(t, c.wb)
	n := batchCost(layout.KeyKey(c.logID, 1), nil)
	for _, k := range [][]byte{layout.EntryKey(c.logID, 1), layout.EntryKey(c.logID, 3), layout.KeyKey(c.logID, 3),
		layout.EntryKey(c.logID, 4), layout.KeyKey(c.logID, 4), layout.HeadKey(c.logID)} {
		v, ok := w[string(k)]
		if !ok {
			t.Fatalf("writer has no key %x", k)
		}
		n += batchCost(k, []byte(v))
	}
	return n
}

// TestAdoptTooLarge sets adoptLimit to the exact cost of the batch, which is refused, then one byte above it, which is accepted.
func TestAdoptTooLarge(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		b := st.open(t)
		s := openStore(t, b)
		defer func(v uint64) { adoptLimit = v }(adoptLimit)
		adoptLimit = adoptCost(t, c)
		if l, err := s.Adopt(c.id, c.rep, utf8); !errors.Is(err, ErrTooLarge) {
			if l != nil {
				l.Close()
			}
			t.Fatalf("Adopt of a batch at the limit: %v", err)
		}
		c.unchanged(t, b)
		adoptLimit++
		l, err := s.Adopt(c.id, c.rep, utf8)
		if err != nil {
			t.Fatalf("Adopt of a batch one byte under the limit: %v", err)
		}
		l.Close()
	})
}

func TestBatchLimits(t *testing.T) {
	want := uint64(1<<32 - 1)
	if strconv.IntSize == 32 {
		want = 1<<31 - 1
	}
	if pebbleBatchLimit != want || adoptLimit != (want+1)/2 {
		t.Fatalf("Pebble limit %d and Adopt limit %d, want %d and half", pebbleBatchLimit, adoptLimit, want)
	}
	if got := batchCost([]byte("ab"), []byte("cde")); got != 16 {
		t.Fatalf("batchCost = %d, want 16", got)
	}
}

func TestPebbleBatchTooLarge(t *testing.T) {
	b, err := OpenPebble(pebbleDir(t))
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	k, v := []byte("k"), bytes.Repeat([]byte{7}, 100)
	defer func(v uint64) { pebbleBatchLimit = v }(pebbleBatchLimit)
	pebbleBatchLimit = pebbleBatchHeader + batchCost(k, v)
	if err := b.Put(k, v); !errors.Is(err, ErrTooLarge) {
		t.Fatalf("Put at the limit: %v", err)
	}
	if _, err := b.Get(k); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Get after the refused Put: %v", err)
	}
	pebbleBatchLimit++
	if err := b.Put(k, v); err != nil {
		t.Fatalf("Put one byte under the limit: %v", err)
	}
	if got, err := b.Get(k); err != nil || !bytes.Equal(got, v) {
		t.Fatalf("Get = %d bytes, %v", len(got), err)
	}
}

// TestAdoptStoreClose closes the store with an adopted log open: the log is closed, and a new store opens it.
func TestAdoptStoreClose(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		s := New(st.open(t))
		l, err := s.Adopt(c.id, c.rep, utf8)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.OpenReplica(c.id.PublicKey(), c.logID, nil); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("OpenReplica of the adopted log: %v", err)
		}
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := l.Append("x"); !errors.Is(err, ErrClosed) {
			t.Fatalf("Append after the store closed: %v", err)
		}
		l, err = openStore(t, st.open(t)).Open(c.id, c.logID, utf8)
		if err != nil {
			t.Fatalf("Open after the store closed: %v", err)
		}
		defer l.Close()
		if l.Len() != 5 {
			t.Fatalf("Len %d, want 5", l.Len())
		}
	})
}

func TestAdoptIntoEmptyLog(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		c := newAdoptCase(t, st)
		b := st.open(t)
		s := openStore(t, b)
		if err := s.Delete(c.logID); err != nil {
			t.Fatal(err)
		}
		for k, v := range snapshot(t, c.at0) {
			putRaw(t, b, []byte(k), []byte(v))
		}
		l, err := s.Adopt(c.id, c.rep, utf8)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		if l.Len() != 5 || !sameTree(mustHead(t, l), c.heads[5]) {
			t.Fatalf("adopted log has Len %d", l.Len())
		}
		for k, v := range snapshot(t, c.wb) {
			if got, err := b.Get([]byte(k)); err != nil || !bytes.Equal(got, []byte(v)) {
				t.Fatalf("adopted log differs from the writer's at key %x", k)
			}
		}
	})
}

// FuzzAdoptBackend puts and deletes arbitrary pairs in the stored log, which deleted entry 2, before an Adopt of a replica that
// deleted entry 1 or of one with every entry live. Adopt must not panic, and a log it returns holds the replica's tree.
func FuzzAdoptBackend(f *testing.F) {
	c := newAdoptCase(f, kinds[0].new(f))
	reps := []*Replica{c.rep, c.replicaOf(f, c.header, c.heads[5], c.live5)}
	b := c.st.open(f)
	d, err := openStore(f, b).Open(c.id, c.logID, utf8)
	if err != nil {
		f.Fatal(err)
	}
	if err := d.Delete(2); err != nil {
		f.Fatal(err)
	}
	d.Close()
	base := snapshot(f, b)
	marker := []byte(base[string(layout.EntryKey(c.logID, 2))])
	f.Add([]byte{0})
	f.Add([]byte{1})
	f.Add(appendPair([]byte{1}, 0, layout.EntryKey(c.logID, 2)[layout.LogPrefixLen:], append(marker[:len(marker)-1:len(marker)-1], 0)))
	f.Add(appendPair([]byte{0}, 1, layout.KeyKey(c.logID, 0)[layout.LogPrefixLen:], nil))
	f.Add(appendPair([]byte{1}, 0, append(layout.KeyKey(c.logID, 1)[layout.LogPrefixLen:], 0), make([]byte, layout.KeySize)))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) == 0 {
			return
		}
		rep := reps[int(data[0])%len(reps)]
		m := NewMemory()
		for k, v := range base {
			putRaw(t, m, []byte(k), []byte(v))
		}
		for data = data[1:]; len(data) > 0; {
			mode := data[0]
			var key, value []byte
			key, data = nextChunk(data[1:])
			value, data = nextChunk(data)
			key = append(layout.LogPrefix(c.logID), key...)
			var err error
			if mode%2 == 0 {
				err = m.Put(key, value)
			} else {
				err = m.Delete(key)
			}
			if err != nil {
				t.Fatal(err)
			}
		}
		l, err := New(m).Adopt(c.id, rep, utf8)
		if err != nil {
			return
		}
		defer l.Close()
		if h := mustHead(t, l); l.Len() != 5 || !sameTree(h, c.heads[5]) {
			t.Fatalf("adopted log has Len %d", l.Len())
		}
	})
}

// TestAdoptHoldsRestoreReservation opens the writer's log (same key and ID, another backend) while Adopt runs: it is refused and Adopt succeeds.
func TestAdoptHoldsRestoreReservation(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	s := openStore(t, c.st.open(t))
	var during error
	ran := false
	beforeRestore.set(func() {
		if ran {
			return
		}
		ran = true
		l, err := c.sw.Open(c.id, c.logID, utf8)
		if err == nil {
			l.Close()
		}
		during = err
	})
	defer beforeRestore.set(nil)
	l, err := s.Adopt(c.id, c.rep, utf8)
	if err != nil {
		t.Fatalf("Adopt: %v", err)
	}
	l.Close()
	if !ran || !errors.Is(during, ErrLogInUse) {
		t.Fatalf("Open of the same log during Adopt: %v, want ErrLogInUse", during)
	}
}

// TestAdoptLeavesCallerOptions passes opts with spare capacity: Adopt must not write into the caller's backing array.
func TestAdoptLeavesCallerOptions(t *testing.T) {
	c := newAdoptCase(t, kinds[0].new(t))
	s := openStore(t, c.st.open(t))
	sentinel := kololog.WithTrustedHead(c.heads[5])
	opts := []kololog.Option{utf8, sentinel}
	l, err := s.Adopt(c.id, c.rep, opts[:1:2]...)
	if err != nil {
		t.Fatal(err)
	}
	l.Close()
	if reflect.ValueOf(opts[1]).Pointer() != reflect.ValueOf(sentinel).Pointer() {
		t.Fatal("Adopt wrote into the spare capacity of the caller's options")
	}
}
