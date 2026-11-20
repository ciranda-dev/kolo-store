package kolostore

import (
	"bytes"
	"errors"
	"fmt"
	"sync/atomic"
	"testing"

	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// syncFrom sends r what a sync layer would: the writer's head, a consistency proof and the missing entries.
func syncFrom(t testing.TB, r *Replica, l *Log) error {
	t.Helper()
	h := mustHead(t, l)
	p, err := l.ConsistencyProof(r.Len(), h.Size)
	if err != nil {
		t.Fatal(err)
	}
	var entries []kololog.Entry
	for i := r.Len(); i < h.Size; i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	return r.Update(h, p, entries)
}

func mustOpenReplica(t testing.TB, s *Store, pub []byte, logID kololog.LogID, header []byte) *Replica {
	t.Helper()
	r, err := s.OpenReplica(pub, logID, header)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func newIdentity(t testing.TB) *koloid.Identity {
	t.Helper()
	id, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func TestDeleteDropsStoredKey(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		s := openStore(t, b)
		l := mustCreate(t, s, testIdentity(t))
		mustAppend(t, l, []byte("a"))
		if e := storedEntry(t, b, l.ID(), 0); len(e.Key) != layout.KeySize || e.Ciphertext == nil {
			t.Fatalf("stored entry has a key of %d bytes", len(e.Key))
		}
		if err := l.Delete(0); err != nil {
			t.Fatal(err)
		}
		if e := storedEntry(t, b, l.ID(), 0); e.Key != nil || e.Ciphertext != nil || e.Marker == nil {
			t.Fatal("Delete kept the stored key or ciphertext, or stored no marker")
		}
	})
}

func TestStoredReplica(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := newIdentity(t)
		pub := owner.PublicKey()
		w := mustCreate(t, openStore(t, NewMemory()), owner, kololog.WithValueEncoding(kololog.UTF8))
		header := w.Header().Encode()

		b := st.open(t)
		s := New(b)
		r := mustOpenReplica(t, s, pub, w.ID(), header)
		if _, err := r.Head(); !errors.Is(err, kololog.ErrNoHead) {
			t.Fatalf("Head of a new replica: %v", err)
		}
		for step, n := range []int{0, 2, 3} {
			for i := range n {
				mustAppend(t, w, fmt.Sprintf("step %d entry %d", step, i))
			}
			if err := syncFrom(t, r, w); err != nil {
				t.Fatalf("step %d: %v", step, err)
			}
			if got := getRaw(t, b, layout.HeadKey(w.ID())); !bytes.Equal(got, mustHead(t, w).Encode()) || r.Len() != w.Len() {
				t.Fatalf("step %d: stored head or Len differs from the writer", step)
			}
		}
		rec, _ := w.Record(1)
		if err := w.Delete(1); err != nil {
			t.Fatal(err)
		}
		e, _ := w.Entry(1)
		if err := r.ApplyDelete(e.Marker); err != nil {
			t.Fatal(err)
		}
		if se := storedEntry(t, b, w.ID(), 1); se.Key != nil || se.Ciphertext != nil || !bytes.Equal(se.Marker, e.Marker) || !bytes.Equal(se.Record, rec.Encode()) {
			t.Fatal("ApplyDelete did not store the marker in place of the ciphertext and key")
		}
		r.Close()
		s.Close()

		s = openStore(t, st.open(t))
		if ids, err := s.List(); err != nil || len(ids) != 1 || ids[0] != w.ID() {
			t.Fatalf("List = %v, %v", ids, err)
		}
		r = mustOpenReplica(t, s, pub, w.ID(), nil)
		if r.Len() != 5 || r.Header().Encoding != kololog.UTF8 || !bytes.Equal(mustReplicaHead(t, r).Encode(), mustHead(t, w).Encode()) {
			t.Fatal("reopened replica differs")
		}
		var text string
		if err := r.Read(4, &text); err != nil || text != "step 2 entry 2" {
			t.Fatalf("Read(4) = %q, %v", text, err)
		}
		if _, err := r.Get(1); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("Get of a deleted entry after reopen: %v", err)
		}
		checkReplicaProofs(t, pub, r)
		mustAppend(t, w, "after reopen")
		if err := syncFrom(t, r, w); err != nil {
			t.Fatal(err)
		}
		if got, err := r.Record(5); err != nil || got.Index != 5 {
			t.Fatalf("Record(5) = %v, %v", got, err)
		}
		r.Close()
		r = mustOpenReplica(t, s, pub, w.ID(), header)
		r.Close()
		w.Close() // Delete holds the log ID under every key in the process
		if err := s.Delete(w.ID()); err != nil {
			t.Fatal(err)
		}
		if ids, _ := s.List(); len(ids) != 0 {
			t.Fatal("Delete left the replica listed")
		}
	})
}

func mustReplicaHead(t testing.TB, r *Replica) kololog.Head {
	t.Helper()
	h, err := r.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func checkReplicaProofs(t testing.TB, pub []byte, r *Replica) {
	t.Helper()
	v, err := kololog.VerifyHead(pub, r.ID(), mustReplicaHead(t, r))
	if err != nil {
		t.Fatal(err)
	}
	for i := range r.Len() {
		rec, err := r.Record(i)
		if err != nil {
			t.Fatal(err)
		}
		p, err := r.InclusionProof(i, v.Size())
		if err != nil || !kololog.VerifyInclusion(v, rec, p) {
			t.Fatalf("inclusion of %d: %v", i, err)
		}
	}
	if p, err := r.ConsistencyProof(1, v.Size()); err != nil || len(p.Path) == 0 {
		t.Fatalf("ConsistencyProof: %v", err)
	}
}

func TestStoredReplicaRefusalsLeaveDiskUnchanged(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := newIdentity(t)
		w := mustCreate(t, openStore(t, NewMemory()), owner)
		mustAppend(t, w, []byte("a"))
		mustAppend(t, w, []byte("b"))
		old := mustHead(t, w)
		b := st.open(t)
		s := openStore(t, b)
		r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
		if err := syncFrom(t, r, w); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, w, []byte("c"))
		h := mustHead(t, w)
		p, _ := w.ConsistencyProof(2, 3)
		e, _ := w.Entry(2)
		other, _ := w.Entry(0)

		badHead := h
		badHead.Root[0] ^= 1
		badProof := p
		badProof.Path = append([][kololog.HashSize]byte{{1}}, p.Path[1:]...)
		wrongKey := e
		wrongKey.Key = other.Key
		before := snapshot(t, b)
		for name, c := range map[string]struct {
			h       kololog.Head
			p       kololog.ConsistencyProof
			entries []kololog.Entry
			want    error
		}{
			"tampered head":                       {badHead, p, []kololog.Entry{e}, kololog.ErrInvalidHead},
			"bad consistency":                     {h, badProof, []kololog.Entry{e}, kololog.ErrBadProof},
			"wrong key":                           {h, p, []kololog.Entry{wrongKey}, kololog.ErrBadEntry},
			"no entries, proof from another size": {h, kololog.ConsistencyProof{OldSize: 1, NewSize: 3}, nil, kololog.ErrBadProof},
			"same head again":                     {old, kololog.ConsistencyProof{OldSize: 2, NewSize: 2}, nil, nil},
		} {
			err := r.Update(c.h, c.p, c.entries)
			if !errors.Is(err, c.want) {
				t.Errorf("%s: Update = %v, want %v", name, err, c.want)
			}
			if !sameSnapshot(before, snapshot(t, b)) {
				t.Errorf("%s: Update changed the stored replica", name)
			}
		}
		if err := r.ApplyDelete([]byte("not a marker")); !errors.Is(err, kololog.ErrMalformed) || !sameSnapshot(before, snapshot(t, b)) {
			t.Errorf("bad marker: %v, or the stored replica changed", err)
		}
		if err := r.Update(h, p, []kololog.Entry{e}); err != nil {
			t.Fatal(err)
		}
		before = snapshot(t, b)
		if err := r.Update(old, kololog.ConsistencyProof{}, nil); !errors.Is(err, kololog.ErrRollback) || !sameSnapshot(before, snapshot(t, b)) {
			t.Fatalf("smaller head: %v, want ErrRollback and no change on disk", err)
		}
	})
}

func sameSnapshot(a, b map[string]string) bool {
	if len(a) != len(b) {
		return false
	}
	for k, v := range a {
		if w, ok := b[k]; !ok || w != v {
			return false
		}
	}
	return true
}

func TestOpenReplicaCorrupt(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := testIdentity(t)
		for _, c := range corruptCases {
			t.Run(c.name, func(t *testing.T) {
				w := mustCreate(t, openStore(t, NewMemory()), owner)
				b := st.open(t)
				s := openStore(t, b)
				r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
				mustAppend(t, w, []byte("a"))
				mustAppend(t, w, []byte("b"))
				if err := syncFrom(t, r, w); err != nil {
					t.Fatal(err)
				}
				old := getRaw(t, b, layout.HeadKey(w.ID()))
				mustAppend(t, w, []byte("c"))
				if err := syncFrom(t, r, w); err != nil {
					t.Fatal(err)
				}
				r.Close()
				c.damage(t, b, w.ID(), old)
				if c.name == "key removed" {
					// A replica may hold an entry without its key.
					got, err := s.OpenReplica(owner.PublicKey(), w.ID(), nil)
					if err != nil {
						t.Fatal(err)
					}
					defer got.Close()
					if _, err := got.Get(1); !errors.Is(err, kololog.ErrKeyUnavailable) {
						t.Fatalf("Get of the entry whose key was removed: %v", err)
					}
					return
				}
				for range 2 {
					got, err := s.OpenReplica(owner.PublicKey(), w.ID(), nil)
					if got != nil || !errors.Is(err, c.want) || errors.Is(err, kololog.ErrOutOfRange) {
						t.Fatalf("OpenReplica = %v, %v, want no replica and %v", got, err, c.want)
					}
				}
			})
		}
	})
}

func TestOpenReplicaErrors(t *testing.T) {
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	other := mustCreate(t, openStore(t, NewMemory()), owner)
	s := openStore(t, NewMemory())
	pub := owner.PublicKey()
	refused := func(name string, want error, pub []byte, header []byte) {
		t.Helper()
		r, err := s.OpenReplica(pub, w.ID(), header)
		if r != nil {
			r.Close()
		}
		if r != nil || !errors.Is(err, want) {
			t.Errorf("%s: OpenReplica = %v, want %v", name, err, want)
		}
	}
	refused("short key", kololog.ErrMalformed, pub[:31], w.Header().Encode())
	refused("create without a header", kololog.ErrMissingHeader, pub, nil)
	refused("header of another owner", kololog.ErrBadLogHeader, testIdentity(t).PublicKey(), w.Header().Encode())
	refused("header of another log", kololog.ErrBadLogHeader, pub, other.Header().Encode())
	if ids, _ := s.List(); len(ids) != 0 {
		t.Fatal("a refused OpenReplica stored something")
	}
	r := mustOpenReplica(t, s, pub, w.ID(), w.Header().Encode())
	refused("second open", ErrLogInUse, pub, nil)
	r.Close()
	changed := w.Header()
	changed.Signature = bytes.Clone(changed.Signature)
	changed.Signature[0] ^= 1
	refused("header other than the stored one", kololog.ErrBadLogHeader, pub, changed.Encode())
	refused("stored replica opened with another key", kololog.ErrBadLogHeader, testIdentity(t).PublicKey(), nil)
}

// TestReplicaKind checks that a stored replica never opens as a writable log, and that an own log never opens as a replica.
func TestReplicaKind(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner, me := newIdentity(t), newIdentity(t)
		w := mustCreate(t, openStore(t, NewMemory()), owner)
		mustAppend(t, w, []byte("a"))
		b := st.open(t)
		s := New(b)
		r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
		if err := syncFrom(t, r, w); err != nil {
			t.Fatal(err)
		}
		r.Close()
		mine := mustCreate(t, s, me)
		mineID := mine.ID()
		mine.Close()
		w.Close() // the owner's writer would hold the log ID in this process
		s.Close()

		b = st.open(t)
		s = openStore(t, b)
		if l, err := s.Open(owner, w.ID()); l != nil || !errors.Is(err, ErrReplica) {
			t.Fatalf("Open of a stored replica with the owner's identity: %v, want ErrReplica", err)
		}
		if r, err := s.OpenReplica(me.PublicKey(), mineID, nil); r != nil || !errors.Is(err, ErrNotReplica) {
			t.Fatalf("OpenReplica of an own log: %v, want ErrNotReplica", err)
		}
		ids, err := s.List()
		if err != nil || len(ids) != 2 {
			t.Fatalf("List = %v, %v", ids, err)
		}
		for _, c := range []struct {
			id      kololog.LogID
			pub     []byte
			replica bool
		}{{w.ID(), owner.PublicKey(), true}, {mineID, me.PublicKey(), false}} {
			pub, replica, err := s.Owner(c.id)
			if err != nil || !bytes.Equal(pub, c.pub) || replica != c.replica {
				t.Fatalf("Owner(%s) = %x, %v, %v", c.id, pub, replica, err)
			}
		}
		if _, _, err := s.Owner(kololog.LogID{9}); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Owner of an unknown log: %v, want ErrNotFound", err)
		}
		r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
		if got, err := r.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("replica after restart: %q, %v", got, err)
		}
		r.Close()

		putRaw(t, b, layout.ListKey(w.ID()), []byte{2})
		if _, err := s.List(); !errors.Is(err, ErrCorrupt) {
			t.Errorf("List with a bad list value: %v", err)
		}
		if _, _, err := s.Owner(w.ID()); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Owner with a bad list value: %v", err)
		}
		if _, err := s.OpenReplica(owner.PublicKey(), w.ID(), nil); !errors.Is(err, ErrCorrupt) {
			t.Errorf("OpenReplica with a bad list value: %v", err)
		}
		putRaw(t, b, layout.ListKey(mineID), layout.ReplicaKind)
		if _, err := s.Open(me, mineID); !errors.Is(err, ErrReplica) {
			t.Errorf("Open of an own log marked as a replica: %v", err)
		}
	})
}

func TestStoredReplicaCommitFails(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := newIdentity(t)
		w := mustCreate(t, openStore(t, NewMemory()), owner)
		mustAppend(t, w, []byte("a"))
		fail := new(atomic.Bool)
		s := openStore(t, failing{st.open(t), fail})
		r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
		if err := syncFrom(t, r, w); err != nil {
			t.Fatal(err)
		}
		mustAppend(t, w, []byte("b"))
		fail.Store(true)
		if err := syncFrom(t, r, w); !errors.Is(err, errInjected) {
			t.Fatalf("Update with a failing commit: %v", err)
		}
		if r.Len() != 1 {
			t.Fatalf("a failed Update changed the replica: Len %d", r.Len())
		}
		fail.Store(false)
		if got, err := r.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("Get after a failed Update: %q, %v", got, err)
		}
		r.Close()
		r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
		if r.Len() != 1 {
			t.Fatalf("reopened replica has %d entries, want the stored 1", r.Len())
		}
		if err := w.Delete(0); err != nil {
			t.Fatal(err)
		}
		e, _ := w.Entry(0)
		fail.Store(true)
		if err := r.ApplyDelete(e.Marker); !errors.Is(err, errInjected) {
			t.Fatalf("ApplyDelete with a failing commit: %v", err)
		}
		fail.Store(false)
		if got, err := r.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("Get after a failed ApplyDelete: %q, %v", got, err)
		}
		r.Close()
		r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
		if got, err := r.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("reopened after a failed ApplyDelete: %q, %v", got, err)
		}
		if err := r.ApplyDelete(e.Marker); err != nil {
			t.Fatal(err)
		}
	})
}

func TestStoreCloseClosesReplicas(t *testing.T) {
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	s := New(NewMemory())
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err := r.Head(); !errors.Is(err, ErrClosed) {
		t.Fatalf("Head after Store.Close: %v", err)
	}
	if err := syncFrom(t, r, w); !errors.Is(err, ErrClosed) {
		t.Fatalf("Update after Store.Close: %v", err)
	}
}

// TestThreeOwnersReplicateEachOther has three owners, each with its own store, log and read-only replicas of the other two logs.
func TestThreeOwnersReplicateEachOther(t *testing.T) {
	for _, kind := range kinds {
		t.Run(kind.name, func(t *testing.T) { threeOwners(t, kind.new) })
	}
}

func threeOwners(t *testing.T, newStorage func(testing.TB) storage) {
	type node struct {
		id       *koloid.Identity
		st       storage
		s        *Store
		log      *Log
		replicas map[int]*Replica
	}
	nodes := make([]*node, 3)
	for i := range nodes {
		n := &node{id: newIdentity(t), st: newStorage(t), replicas: map[int]*Replica{}}
		n.s = New(n.st.open(t))
		n.log = mustCreate(t, n.s, n.id, kololog.WithValueEncoding(kololog.UTF8))
		nodes[i] = n
	}
	for i, n := range nodes {
		for j, m := range nodes {
			if i != j {
				n.replicas[j] = mustOpenReplica(t, n.s, m.id.PublicKey(), m.log.ID(), m.log.Header().Encode())
			}
		}
	}
	for round := range 3 {
		for i, n := range nodes {
			mustAppend(t, n.log, fmt.Sprintf("owner %d message %d", i, round))
		}
		for _, n := range nodes {
			for j, r := range n.replicas {
				if err := syncFrom(t, r, nodes[j].log); err != nil {
					t.Fatalf("round %d, log %d: %v", round, j, err)
				}
			}
		}
	}
	logIDs := make([]kololog.LogID, 3)
	for i, n := range nodes {
		logIDs[i] = n.log.ID()
		if err := n.s.Close(); err != nil {
			t.Fatal(err)
		}
	}
	for i, n := range nodes {
		s := openStore(t, n.st.open(t))
		own, err := s.Open(n.id, logIDs[i], kololog.WithValueEncoding(kololog.UTF8))
		if err != nil {
			t.Fatal(err)
		}
		for j := range nodes {
			var read func(uint64, any) error = own.Read
			if j != i {
				r := mustOpenReplica(t, s, nodes[j].id.PublicKey(), logIDs[j], nil)
				read = r.Read
			}
			for k := range uint64(3) {
				var got string
				if err := read(k, &got); err != nil || got != fmt.Sprintf("owner %d message %d", j, k) {
					t.Errorf("owner %d reads log %d entry %d = %q, %v", i, j, k, got, err)
				}
			}
		}
	}
}

// syncChunk sends r entries [r.Len(), to) of l with the proof from to up to l's head.
func syncChunk(t testing.TB, r *Replica, l *Log, to uint64) error {
	t.Helper()
	h := mustHead(t, l)
	p, err := l.ConsistencyProof(to, h.Size)
	if err != nil {
		t.Fatal(err)
	}
	var entries []kololog.Entry
	for i := r.Len(); i < to; i++ {
		e, err := l.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		entries = append(entries, e)
	}
	return r.Update(h, p, entries)
}

func TestStoredReplicaCatchUp(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := newIdentity(t)
		w := mustCreate(t, openStore(t, NewMemory()), owner)
		for i := range 20 {
			mustAppend(t, w, []byte(fmt.Sprintf("entry %d", i)))
		}
		pub := owner.PublicKey()
		b := st.open(t)
		s := New(b)
		r := mustOpenReplica(t, s, pub, w.ID(), w.Header().Encode())
		if err := syncChunk(t, r, w, 5); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, b)
		h := mustHead(t, w)
		p, _ := w.ConsistencyProof(12, 20)
		p.Path[0][0] ^= 1
		var chunk []kololog.Entry
		for i := uint64(5); i < 12; i++ {
			e, _ := w.Entry(i)
			chunk = append(chunk, e)
		}
		if err := r.Update(h, p, chunk); !errors.Is(err, kololog.ErrBadProof) || !sameSnapshot(before, snapshot(t, b)) {
			t.Fatalf("chunk with a bad proof: %v, or the disk changed", err)
		}
		if err := syncChunk(t, r, w, 12); err != nil {
			t.Fatal(err)
		}
		if th, tp, ok := r.Target(); !ok || !bytes.Equal(th.Encode(), h.Encode()) || tp.OldSize != 12 || tp.NewSize != 20 {
			t.Fatalf("Target after a chunk = %d, %d to %d, %v", th.Size, tp.OldSize, tp.NewSize, ok)
		}
		r.Close()
		s.Close()

		b = st.open(t)
		s = openStore(t, b)
		r = mustOpenReplica(t, s, pub, w.ID(), nil)
		if r.Len() != 12 {
			t.Fatalf("Len after a restart in a catch-up = %d, want 12", r.Len())
		}
		if _, err := r.Head(); !errors.Is(err, kololog.ErrNoHead) {
			t.Fatalf("Head during the first catch-up: %v", err)
		}
		if got, err := r.Get(11); err != nil || string(got) != "entry 11" {
			t.Fatalf("Get(11) after the restart: %q, %v", got, err)
		}
		if th, tp, ok := r.Target(); !ok || !bytes.Equal(th.Encode(), h.Encode()) || tp.OldSize != 12 || tp.NewSize != 20 {
			t.Fatalf("Target after a restart = %d, %d to %d, %v", th.Size, tp.OldSize, tp.NewSize, ok)
		}
		if err := syncChunk(t, r, w, 20); err != nil {
			t.Fatal(err)
		}
		if _, _, ok := r.Target(); ok {
			t.Fatal("Target still set after the catch-up")
		}
		if _, err := b.Get(layout.TargetKey(w.ID())); !errors.Is(err, ErrNotFound) {
			t.Fatalf("finished catch-up left its target stored: %v", err)
		}
		r.Close()
		r = mustOpenReplica(t, s, pub, w.ID(), nil)
		defer r.Close()
		if got := mustReplicaHead(t, r); !bytes.Equal(got.Encode(), h.Encode()) || r.Len() != 20 {
			t.Fatal("reopened replica does not hold the full head")
		}
	})
}

func TestStoredCatchUpCorrupt(t *testing.T) {
	cases := []corruptCase{
		{"tampered target", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
			putRaw(t, b, layout.TargetKey(id), flipLast(getRaw(t, b, layout.TargetKey(id))))
		}, ErrCorrupt},
		{"target cut", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
			v := getRaw(t, b, layout.TargetKey(id))
			putRaw(t, b, layout.TargetKey(id), v[:len(v)-1])
		}, ErrCorrupt},
		{"entry past the head removed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
			delRaw(t, b, layout.EntryKey(id, 6))
		}, ErrCorrupt},
		{"target removed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
			delRaw(t, b, layout.TargetKey(id))
		}, ErrCorrupt},
		{"target proof starting below the head, entries removed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
			hb, _, err := layout.DecodeTarget(getRaw(t, b, layout.TargetKey(id)))
			if err != nil {
				t.Fatal(err)
			}
			h, _ := kololog.DecodeHead(hb)
			putRaw(t, b, layout.TargetKey(id), layout.EncodeTarget(hb, kololog.ConsistencyProof{OldSize: 1, NewSize: h.Size}.Encode()))
			for i := uint64(1); i < 8; i++ {
				delRaw(t, b, layout.EntryKey(id, i))
			}
		}, ErrCorrupt},
		{"entry past the head tampered", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
			e := storedEntry(t, b, id, 6)
			flipLast(e.Ciphertext)
			putStored(t, b, id, 6, e)
		}, ErrCorrupt},
	}
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := testIdentity(t)
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				w := mustCreate(t, openStore(t, NewMemory()), owner)
				for range 4 {
					mustAppend(t, w, []byte("a"))
				}
				b := st.open(t)
				s := openStore(t, b)
				r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
				if err := syncFrom(t, r, w); err != nil {
					t.Fatal(err)
				}
				for range 6 {
					mustAppend(t, w, []byte("b"))
				}
				if err := syncChunk(t, r, w, 8); err != nil {
					t.Fatal(err)
				}
				r.Close()
				c.damage(t, b, w.ID(), nil)
				got, err := s.OpenReplica(owner.PublicKey(), w.ID(), nil)
				if got != nil || !errors.Is(err, c.want) {
					t.Fatalf("OpenReplica = %v, %v, want %v", got, err, c.want)
				}
			})
		}
	})
}

func TestTargetKeyOnOwnLog(t *testing.T) {
	b := NewMemory()
	s := openStore(t, b)
	id := testIdentity(t)
	l := mustCreate(t, s, id)
	mustAppend(t, l, []byte("a"))
	l.Close()
	putRaw(t, b, layout.TargetKey(l.ID()), layout.EncodeTarget(nil, nil))
	if _, err := s.Open(id, l.ID()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open of an own log with a target key: %v, want ErrCorrupt", err)
	}
}

func TestStoredReplicaAddKey(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		owner := newIdentity(t)
		w := mustCreate(t, openStore(t, NewMemory()), owner)
		mustAppend(t, w, []byte("a"))
		mustAppend(t, w, []byte("b"))
		mustAppend(t, w, []byte("c"))
		if err := w.Delete(2); err != nil {
			t.Fatal(err)
		}
		h := mustHead(t, w)
		var entries []kololog.Entry
		var keys [][]byte
		for i := range uint64(3) {
			e, _ := w.Entry(i)
			keys = append(keys, bytes.Clone(e.Key))
			e.Key = nil
			entries = append(entries, e)
		}
		b := st.open(t)
		s := New(b)
		r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
		if err := r.Update(h, kololog.ConsistencyProof{}, entries); err != nil {
			t.Fatal(err)
		}
		before := snapshot(t, b)
		if err := r.AddKey(0, keys[1]); !errors.Is(err, kololog.ErrBadEntry) || !sameSnapshot(before, snapshot(t, b)) {
			t.Fatalf("wrong key: %v, or the disk changed", err)
		}
		if err := r.AddKey(2, keys[0]); !errors.Is(err, kololog.ErrDeleted) || !sameSnapshot(before, snapshot(t, b)) {
			t.Fatalf("key for a deleted entry: %v, or the disk changed", err)
		}
		if err := r.AddKey(0, keys[0]); err != nil {
			t.Fatal(err)
		}
		if e := storedEntry(t, b, w.ID(), 0); !bytes.Equal(e.Key, keys[0]) {
			t.Fatal("AddKey did not store the key")
		}
		before = snapshot(t, b)
		writes := 0
		r.s.b = countingBatches{r.s.b, &writes}
		if err := r.AddKey(0, keys[0]); err != nil || writes != 0 {
			t.Fatalf("AddKey of the key the entry has: %v, %d writes", err, writes)
		}
		if err := r.AddKey(0, keys[1]); !errors.Is(err, kololog.ErrBadEntry) || writes != 0 || !sameSnapshot(before, snapshot(t, b)) {
			t.Fatalf("AddKey of another key for an entry that has one: %v, %d writes", err, writes)
		}
		r.s.b = r.s.b.(countingBatches).Backend
		r.Close()
		s.Close()
		s = openStore(t, st.open(t))
		r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
		defer r.Close()
		if got, err := r.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("Get(0) after reopen = %q, %v", got, err)
		}
		if _, err := r.Get(1); !errors.Is(err, kololog.ErrKeyUnavailable) {
			t.Fatalf("Get(1) = %v, want ErrKeyUnavailable", err)
		}
	})
}

// countingBatches counts the batches committed through it.
type countingBatches struct {
	Backend
	n *int
}

func (c countingBatches) NewBatch() Batch { *c.n++; return c.Backend.NewBatch() }

// TestHeldHeadAtLenReopens is a head of the size of Len arriving during a catch-up; the replica must still reopen.
func TestHeldHeadAtLenReopens(t *testing.T) {
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	for i := range 5 {
		mustAppend(t, w, []byte(fmt.Sprintf("e%d", i)))
	}
	h5 := mustHead(t, w)
	for i := 5; i < 10; i++ {
		mustAppend(t, w, []byte(fmt.Sprintf("e%d", i)))
	}
	b := NewMemory()
	s := openStore(t, b)
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	if err := syncChunk(t, r, w, 5); err != nil {
		t.Fatal(err)
	}
	if err := r.Update(h5, kololog.ConsistencyProof{}, nil); err != nil {
		t.Fatal(err)
	}
	if _, _, ok := r.Target(); !ok {
		t.Fatal("the held head dropped the pending target")
	}
	before := replicaState(t, r)
	r.Close()
	r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
	defer r.Close()
	if replicaState(t, r) != before {
		t.Fatal("reopened replica differs")
	}
	if err := syncFrom(t, r, w); err != nil {
		t.Fatal(err)
	}
}

func TestStoredTargetWithoutEntriesReopens(t *testing.T) {
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	for range 3 {
		mustAppend(t, w, []byte("a"))
	}
	s := openStore(t, NewMemory())
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	if err := syncFrom(t, r, w); err != nil {
		t.Fatal(err)
	}
	mustAppend(t, w, []byte("b"))
	h := mustHead(t, w)
	p, _ := w.ConsistencyProof(3, 4)
	if err := r.Update(h, p, nil); err != nil {
		t.Fatal(err)
	}
	before := replicaState(t, r)
	r.Close()
	r = mustOpenReplica(t, s, owner.PublicKey(), w.ID(), nil)
	defer r.Close()
	if replicaState(t, r) != before {
		t.Fatal("a target with no entries past the head did not survive a reopen")
	}
	if err := syncFrom(t, r, w); err != nil {
		t.Fatal(err)
	}
}

func TestOwnLogEntryWithoutKey(t *testing.T) {
	b := NewMemory()
	s := openStore(t, b)
	id := testIdentity(t)
	l := mustCreate(t, s, id)
	mustAppend(t, l, []byte("a"))
	l.Close()
	e := storedEntry(t, b, l.ID(), 0)
	e.Key = nil
	putStored(t, b, l.ID(), 0, e)
	if got, err := s.Open(id, l.ID()); got != nil || !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Open of an own log whose entry lost its key: %v, want ErrCorrupt", err)
	}
}

func TestDamagedHeaderOrHeadIsCorrupt(t *testing.T) {
	for name, c := range map[string]struct {
		key  func(kololog.LogID) []byte
		want error
	}{
		"header": {layout.HeaderKey, kololog.ErrBadLogHeader},
		"head":   {layout.HeadKey, kololog.ErrInvalidHead},
	} {
		b := NewMemory()
		s := openStore(t, b)
		id := testIdentity(t)
		l := mustCreate(t, s, id)
		mustAppend(t, l, []byte("a"))
		l.Close()
		putRaw(t, b, c.key(l.ID()), flipLast(getRaw(t, b, c.key(l.ID()))))
		if _, err := s.Open(id, l.ID()); !errors.Is(err, ErrCorrupt) || !errors.Is(err, c.want) {
			t.Errorf("%s damaged: %v, want ErrCorrupt and %v", name, err, c.want)
		}
		delRaw(t, b, layout.HeaderKey(l.ID()))
		if _, err := s.Open(id, l.ID()); !errors.Is(err, ErrCorrupt) || !errors.Is(err, kololog.ErrMissingHeader) {
			t.Errorf("listed log without a header: %v", err)
		}
	}
}

// keepingBackend keeps every value it returns from Get, hands to Scan callbacks or is given in a batch, to check that the store clears them.
type keepingBackend struct {
	Backend
	kept *[][]byte
}

func (k keepingBackend) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	return k.Backend.Scan(start, end, reverse, limit, func(key, value []byte) bool {
		*k.kept = append(*k.kept, value)
		return fn(key, value)
	})
}

func (k keepingBackend) Get(key []byte) ([]byte, error) {
	v, err := k.Backend.Get(key)
	*k.kept = append(*k.kept, v)
	return v, err
}

func (k keepingBackend) NewBatch() Batch { return keepingBatch{k.Backend.NewBatch(), k.kept} }

type keepingBatch struct {
	Batch
	kept *[][]byte
}

func (k keepingBatch) Put(key, value []byte) {
	*k.kept = append(*k.kept, value)
	k.Batch.Put(key, value)
}

// liveKeys counts kept values that still decode to an entry with a key.
func liveKeys(kept [][]byte) int {
	n := 0
	for _, v := range kept {
		if e, err := layout.DecodeEntry(v); err == nil && len(e.Key) > 0 {
			n++
		}
	}
	return n
}

func TestKeyCopiesCleared(t *testing.T) {
	var kept [][]byte
	s := New(keepingBackend{NewMemory(), &kept})
	defer s.Close()
	id := testIdentity(t)
	l := mustCreate(t, s, id)
	mustAppend(t, l, []byte("a"))
	mustAppend(t, l, []byte("b"))
	if n := liveKeys(kept); n != 0 {
		t.Errorf("Append left %d entry values with a key", n)
	}
	l.Close()
	kept = nil
	l, err := s.Open(id, l.ID())
	if err != nil {
		t.Fatal(err)
	}
	if n := liveKeys(kept); n != 0 {
		t.Errorf("Open left %d scanned values with a key", n)
	}
	if got, err := l.Get(1); err != nil || string(got) != "b" {
		t.Fatalf("Get(1) = %q, %v", got, err)
	}
	if n := liveKeys(kept); n != 0 {
		t.Errorf("Get left %d read values with a key", n)
	}
	e, err := l.Entry(0)
	if err != nil {
		t.Fatal(err)
	}
	clear(e.Key)
	l.Close()

	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	mustAppend(t, w, []byte("x"))
	mustAppend(t, w, []byte("y"))
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	defer r.Close()
	h := mustHead(t, w)
	e0, _ := w.Entry(0)
	e1, _ := w.Entry(1)
	k1 := bytes.Clone(e1.Key)
	e1.Key = nil
	kept = nil
	if err := r.Update(h, kololog.ConsistencyProof{}, []kololog.Entry{e0, e1}); err != nil {
		t.Fatal(err)
	}
	if err := r.AddKey(1, k1); err != nil {
		t.Fatal(err)
	}
	if n := liveKeys(kept); n != 0 {
		t.Errorf("Update or AddKey left %d entry values with a key", n)
	}
}
