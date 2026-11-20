package kolostore

import (
	"bytes"
	"encoding/json"
	"errors"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	koloid "github.com/ciranda-dev/kolo-id"
	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

func testIdentity(t testing.TB) *koloid.Identity {
	t.Helper()
	id, err := koloid.FromSeed(bytes.Repeat([]byte{7}, 32))
	if err != nil {
		t.Fatal(err)
	}
	return id
}

func openStore(t testing.TB, b Backend) *Store {
	s := New(b)
	t.Cleanup(func() { s.Close() })
	return s
}

func mustCreate(t testing.TB, s *Store, id *koloid.Identity, opts ...kololog.Option) *Log {
	t.Helper()
	l, err := s.Create(id, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return l
}

func mustAppend(t testing.TB, l *Log, v any) uint64 {
	t.Helper()
	i, err := l.Append(v)
	if err != nil {
		t.Fatal(err)
	}
	return i
}

func mustHead(t testing.TB, l *Log) kololog.Head {
	t.Helper()
	h, err := l.Head()
	if err != nil {
		t.Fatal(err)
	}
	return h
}

func sameTree(a, b kololog.Head) bool {
	return a.LogID == b.LogID && a.Size == b.Size && a.Root == b.Root
}

func TestCreateAppendReopen(t *testing.T) {
	type note struct {
		Text string `json:"text"`
	}
	encodings := []struct {
		enc    kololog.ValueEncoding
		values []any
		out    func() any
	}{
		{kololog.Bytes, []any{[]byte("a"), []byte{}, []byte("c")}, func() any { return new([]byte) }},
		{kololog.UTF8, []any{"a", "", "café"}, func() any { return new(string) }},
		{kololog.JSON, []any{note{"a"}, note{}, note{"c"}}, func() any { return new(note) }},
	}
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		for _, e := range encodings {
			opt := kololog.WithValueEncoding(e.enc)
			s := openStore(t, st.open(t))
			l := mustCreate(t, s, id, opt)
			logID := l.ID()
			for i, v := range e.values {
				if got := mustAppend(t, l, v); got != uint64(i) {
					t.Fatalf("%s: Append returned %d, want %d", e.enc, got, i)
				}
			}
			out := e.out()
			if err := l.Read(2, out); err != nil {
				t.Fatalf("%s: Read before reopen: %v", e.enc, err)
			}
			head := mustHead(t, l)
			if err := l.Close(); err != nil {
				t.Fatal(err)
			}
			if err := s.Close(); err != nil {
				t.Fatal(err)
			}

			s = openStore(t, st.open(t))
			if ids, err := s.List(); err != nil || !slices.Contains(ids, logID) {
				t.Fatalf("%s: List = %v, %v", e.enc, ids, err)
			}
			if e.enc != kololog.Bytes {
				if _, err := s.Open(id, logID); !errors.Is(err, kololog.ErrEncodingMismatch) {
					t.Fatalf("%s: Open without the encoding option: %v", e.enc, err)
				}
			}
			l, err := s.Open(id, logID, opt)
			if err != nil {
				t.Fatalf("%s: Open: %v", e.enc, err)
			}
			if l.Len() != 3 || !sameTree(mustHead(t, l), head) || l.Header().Encoding != e.enc {
				t.Fatalf("%s: reopened log has Len %d and another head", e.enc, l.Len())
			}
			for i := range uint64(3) {
				out := e.out()
				if err := l.Read(i, out); err != nil {
					t.Fatalf("%s: Read(%d) of an entry written before Open: %v", e.enc, i, err)
				}
				got, _ := json.Marshal(out)
				if want, _ := json.Marshal(e.values[i]); string(got) != string(want) {
					t.Fatalf("%s: Read(%d) after reopen = %s, want %s", e.enc, i, got, want)
				}
			}
			if got := mustAppend(t, l, e.values[0]); got != 3 {
				t.Fatalf("%s: Append after Open returned %d, want 3", e.enc, got)
			}
			out = e.out()
			if err := l.Read(3, out); err != nil {
				t.Fatalf("%s: Read new entry: %v", e.enc, err)
			}
			checkConsistency(t, id, l, head)
			if err := l.Delete(1); err != nil {
				t.Fatal(err)
			}
			head = mustHead(t, l)
			l.Close()
			s.Close()

			s = openStore(t, st.open(t))
			l, err = s.Open(id, logID, opt)
			if err != nil {
				t.Fatalf("%s: Open after Delete: %v", e.enc, err)
			}
			if l.Len() != 4 || !sameTree(mustHead(t, l), head) {
				t.Fatalf("%s: second reopen: Len %d, head differs", e.enc, l.Len())
			}
			if _, err := l.Get(1); !errors.Is(err, kololog.ErrDeleted) {
				t.Fatalf("%s: Get of a deleted entry: %v, want ErrDeleted", e.enc, err)
			}
			if err := l.Read(3, e.out()); err != nil {
				t.Fatalf("%s: Read(3) after a second reopen: %v", e.enc, err)
			}
			checkProofs(t, id, l)
			l.Close()
			s.Close()
		}
	})
}

func checkProofs(t testing.TB, id *koloid.Identity, l *Log) {
	t.Helper()
	pub := id.PublicKey()
	h, err := kololog.VerifyHead(pub, l.ID(), mustHead(t, l))
	if err != nil {
		t.Fatal(err)
	}
	for i := range l.Len() {
		rec, err := l.Record(i)
		if err != nil {
			t.Fatal(err)
		}
		p, err := l.InclusionProof(i, h.Size())
		if err != nil || !kololog.VerifyInclusion(h, rec, p) {
			t.Fatalf("inclusion of %d does not verify: %v", i, err)
		}
	}
}

func checkConsistency(t testing.TB, id *koloid.Identity, l *Log, old kololog.Head) {
	t.Helper()
	pub := id.PublicKey()
	o, err := kololog.VerifyHead(pub, l.ID(), old)
	if err != nil {
		t.Fatal(err)
	}
	n, err := kololog.VerifyHead(pub, l.ID(), mustHead(t, l))
	if err != nil {
		t.Fatal(err)
	}
	cp, err := l.ConsistencyProof(o.Size(), n.Size())
	if err != nil || !kololog.VerifyConsistency(o, n, cp) {
		t.Fatalf("consistency %d to %d does not verify: %v", o.Size(), n.Size(), err)
	}
}

var errInjected = errors.New("injected commit failure")

// failing is a backend whose batches are dropped without being written while fail is set, like a crash before Commit.
type failing struct {
	Backend
	fail *atomic.Bool
}

func (f failing) NewBatch() Batch { return failingBatch{f.Backend.NewBatch(), f.fail} }

type failingBatch struct {
	Batch
	fail *atomic.Bool
}

func (b failingBatch) Commit() error {
	if b.fail.Load() {
		return errInjected
	}
	return b.Batch.Commit()
}

func TestCrashBeforeCommit(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		fail := new(atomic.Bool)
		s := openStore(t, failing{st.open(t), fail})
		l := mustCreate(t, s, id)
		mustAppend(t, l, []byte("a"))
		mustAppend(t, l, []byte("b"))
		head := mustHead(t, l)

		fail.Store(true)
		_, err := l.Append([]byte("lost"))
		if !errors.Is(err, errInjected) {
			t.Fatalf("Append with a failing commit: %v", err)
		}
		if l.Len() != 2 || !sameTree(mustHead(t, l), head) {
			t.Errorf("a failed Append changed the log: Len %d", l.Len())
		}
		// A failed write changes nothing, so the log keeps working.
		fail.Store(false)
		if got, err := l.Get(1); err != nil || string(got) != "b" {
			t.Fatalf("Get after a failed Append = %q, %v", got, err)
		}
		if err := l.Close(); err != nil {
			t.Fatal(err)
		}
		logID := l.ID()
		s.Close()

		s = openStore(t, failing{st.open(t), fail})
		l, err = s.Open(id, logID)
		if err != nil {
			t.Fatalf("Open after the crash: %v", err)
		}
		if l.Len() != 2 || !sameTree(mustHead(t, l), head) {
			t.Fatalf("after the crash: Len %d, want the head of size 2", l.Len())
		}

		fail.Store(true)
		if err := l.Delete(1); !errors.Is(err, errInjected) {
			t.Fatalf("Delete with a failing commit: %v", err)
		}
		fail.Store(false)
		if got, err := l.Get(1); err != nil || string(got) != "b" {
			t.Fatalf("entry deleted in the open log by a failed commit: Get = %q, %v, want b", got, err)
		}
		l.Close()
		s.Close()

		s = openStore(t, st.open(t))
		l, err = s.Open(id, logID)
		if err != nil {
			t.Fatal(err)
		}
		if got, err := l.Get(1); err != nil || string(got) != "b" {
			t.Fatalf("entry deleted by a failed commit: Get = %q, %v, want b", got, err)
		}
		if got := mustAppend(t, l, []byte("c")); got != 2 {
			t.Fatalf("Append after recovery returned %d, want 2", got)
		}
	})
}

func TestCreateCommitFails(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		fail := new(atomic.Bool)
		fail.Store(true)
		b := st.open(t)
		s := New(failing{b, fail})
		if _, err := s.Create(testIdentity(t)); !errors.Is(err, errInjected) {
			t.Fatalf("Create with a failing commit: %v", err)
		}
		if ids, err := s.List(); err != nil || len(ids) != 0 {
			t.Fatalf("List after a failed Create = %v, %v", ids, err)
		}
		if n := len(snapshot(t, b)); n != 0 {
			t.Fatalf("failed Create left %d keys", n)
		}
	})
}

// corruptCase builds a log with three entries in b and then damages it.
type corruptCase struct {
	name   string
	damage func(t testing.TB, b Backend, logID kololog.LogID, oldHead []byte)
	want   error
}

func getRaw(t testing.TB, b Backend, k []byte) []byte {
	t.Helper()
	v, err := b.Get(k)
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func putRaw(t testing.TB, b Backend, k, v []byte) {
	t.Helper()
	if err := b.Put(k, v); err != nil {
		t.Fatal(err)
	}
}

func delRaw(t testing.TB, b Backend, k []byte) {
	t.Helper()
	if err := b.Delete(k); err != nil {
		t.Fatal(err)
	}
}

func flipLast(v []byte) []byte {
	v[len(v)-1] ^= 1
	return v
}

var corruptCases = []corruptCase{
	{"head of a smaller size", func(t testing.TB, b Backend, id kololog.LogID, old []byte) {
		putRaw(t, b, layout.HeadKey(id), old)
	}, ErrCorrupt},
	{"tampered head", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.HeadKey(id), flipLast(getRaw(t, b, layout.HeadKey(id))))
	}, kololog.ErrInvalidHead},
	{"head cut", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		h := getRaw(t, b, layout.HeadKey(id))
		putRaw(t, b, layout.HeadKey(id), h[:len(h)-1])
	}, kololog.ErrMalformed},
	{"head missing", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		delRaw(t, b, layout.HeadKey(id))
	}, ErrCorrupt},
	{"header missing", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		delRaw(t, b, layout.HeaderKey(id))
	}, kololog.ErrMissingHeader},
	{"tampered header", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.HeaderKey(id), flipLast(getRaw(t, b, layout.HeaderKey(id))))
	}, kololog.ErrBadLogHeader},
	{"unknown entry version", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		v := getRaw(t, b, layout.EntryKey(id, 1))
		v[0] = 4
		putRaw(t, b, layout.EntryKey(id, 1), v)
	}, ErrCorrupt},
	{"key cut", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		k := getRaw(t, b, layout.KeyKey(id, 1))
		putRaw(t, b, layout.KeyKey(id, 1), k[:len(k)-1])
	}, ErrCorrupt},
	{"key removed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		delRaw(t, b, layout.KeyKey(id, 1))
	}, kololog.ErrRestoreMismatch},
	{"key past the head", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.KeyKey(id, 3), getRaw(t, b, layout.KeyKey(id, 2)))
	}, ErrCorrupt},
	{"stray key among the keys", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, append(layout.KeyKey(id, 0), 0), getRaw(t, b, layout.KeyKey(id, 1)))
	}, ErrCorrupt},
	{"entry value cut", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		v := getRaw(t, b, layout.EntryKey(id, 2))
		putRaw(t, b, layout.EntryKey(id, 2), v[:len(v)-5])
	}, ErrCorrupt},
	{"entry value with trailing bytes", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.EntryKey(id, 0), append(getRaw(t, b, layout.EntryKey(id, 0)), 0))
	}, ErrCorrupt},
	{"tampered ciphertext", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		e := storedEntry(t, b, id, 0)
		flipLast(e.Ciphertext)
		putStored(t, b, id, 0, e)
	}, ErrCorrupt},
	{"tampered key", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.KeyKey(id, 0), flipLast(getRaw(t, b, layout.KeyKey(id, 0))))
	}, kololog.ErrRestoreMismatch},
	{"key of another entry", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		e := storedEntry(t, b, id, 0)
		e.Key = storedEntry(t, b, id, 1).Key
		putStored(t, b, id, 0, e)
	}, ErrCorrupt},
	{"ciphertext grown", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		e, err := decodeEntry(getRaw(t, b, layout.EntryKey(id, 0)))
		if err != nil {
			t.Fatal(err)
		}
		e.Ciphertext = append(e.Ciphertext, 0)
		putStored(t, b, id, 0, e)
	}, ErrCorrupt},
	{"ciphertext far larger than the record", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		e, err := decodeEntry(getRaw(t, b, layout.EntryKey(id, 1)))
		if err != nil {
			t.Fatal(err)
		}
		e.Ciphertext = append(e.Ciphertext, make([]byte, 1<<20)...)
		putStored(t, b, id, 1, e)
	}, ErrCorrupt},
	{"last entry removed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		delRaw(t, b, layout.EntryKey(id, 2))
	}, ErrCorrupt},
	{"middle entry removed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		delRaw(t, b, layout.EntryKey(id, 1))
	}, ErrCorrupt},
	{"entries swapped", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		e0, e1 := getRaw(t, b, layout.EntryKey(id, 0)), getRaw(t, b, layout.EntryKey(id, 1))
		putRaw(t, b, layout.EntryKey(id, 0), e1)
		putRaw(t, b, layout.EntryKey(id, 1), e0)
	}, ErrCorrupt},
	{"extra entry past the head", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.EntryKey(id, 3), getRaw(t, b, layout.EntryKey(id, 2)))
	}, ErrCorrupt},
	{"entry far past the head", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, layout.EntryKey(id, 1<<40), getRaw(t, b, layout.EntryKey(id, 2)))
	}, ErrCorrupt},
	{"unexpected key in the log", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, append(layout.LogPrefix(id), 'x'), nil)
	}, ErrCorrupt},
	{"short entry key", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		putRaw(t, b, append(layout.LogPrefix(id), layout.SuffixEntry, 0), nil)
	}, ErrCorrupt},
	{"not listed", func(t testing.TB, b Backend, id kololog.LogID, _ []byte) {
		delRaw(t, b, layout.ListKey(id))
	}, ErrCorrupt},
}

func TestOpenCorrupt(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		for _, c := range corruptCases {
			t.Run(c.name, func(t *testing.T) {
				b := failing{st.open(t), new(atomic.Bool)}
				s := New(b)
				defer s.Close()
				l := mustCreate(t, s, id)
				mustAppend(t, l, []byte("a"))
				mustAppend(t, l, []byte("b"))
				old := getRaw(t, b, layout.HeadKey(l.ID()))
				mustAppend(t, l, []byte("c"))
				l.Close()
				c.damage(t, b, l.ID(), old)
				for range 2 { // the failed Open must not leave the log reserved
					got, err := s.Open(id, l.ID())
					if got != nil || !errors.Is(err, c.want) || errors.Is(err, kololog.ErrOutOfRange) {
						t.Fatalf("Open = %v, %v, want no log and %v", got, err, c.want)
					}
				}
			})
		}
	})
}

func TestOpenWrongIdentityOrUnknownLog(t *testing.T) {
	forEachBackend(t, testOpenWrongIdentityOrUnknownLog)
}

func testOpenWrongIdentityOrUnknownLog(t *testing.T, st storage) {
	s := openStore(t, st.open(t))
	l := mustCreate(t, s, testIdentity(t))
	l.Close()
	other, err := koloid.Generate()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := s.Open(other, l.ID()); !errors.Is(err, kololog.ErrBadLogHeader) {
		t.Errorf("Open with another identity: %v, want ErrBadLogHeader", err)
	}
	if _, err := s.Open(nil, l.ID()); !errors.Is(err, koloid.ErrInvalidIdentity) {
		t.Errorf("Open with a nil identity: %v", err)
	}
	if _, err := s.Open(testIdentity(t), kololog.LogID{1}); !errors.Is(err, kololog.ErrMissingHeader) {
		t.Errorf("Open of an unknown log: %v, want ErrMissingHeader", err)
	}
}

func TestLogInUse(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		s := openStore(t, st.open(t))
		l := mustCreate(t, s, id)
		if _, err := s.Open(id, l.ID()); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("Open of a created log: %v, want ErrLogInUse", err)
		}
		if err := s.Delete(l.ID()); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("Delete of an open log: %v, want ErrLogInUse", err)
		}
		l.Close()
		if err := l.Close(); err != nil {
			t.Fatalf("second Close: %v", err)
		}
		if _, err := l.Append([]byte("x")); !errors.Is(err, ErrClosed) {
			t.Fatalf("Append after Close: %v, want ErrClosed", err)
		}
		l2, err := s.Open(id, l.ID())
		if err != nil {
			t.Fatal(err)
		}
		if _, err := s.Open(id, l.ID()); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("second Open: %v, want ErrLogInUse", err)
		}
		l2.Close()
	})
}

func TestStoreClose(t *testing.T) {
	forEachBackend(t, testStoreClose)
}

func testStoreClose(t *testing.T, st storage) {
	id := testIdentity(t)
	s := New(st.open(t))
	l := mustCreate(t, s, id)
	mustAppend(t, l, []byte("a"))
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}
	if err := s.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}
	_, errCreate := s.Create(id)
	_, errOpen := s.Open(id, l.ID())
	_, errList := s.List()
	_, errAppend := l.Append([]byte("b"))
	for _, err := range []error{errCreate, errOpen, errList, errAppend, s.Delete(l.ID()), l.Delete(0)} {
		if !errors.Is(err, ErrClosed) {
			t.Errorf("call after Store.Close returned %v, want ErrClosed", err)
		}
	}
	if _, err := l.Get(0); !errors.Is(err, ErrClosed) {
		t.Errorf("read after Store.Close: %v, want ErrClosed", err)
	}
}

func TestDeleteLog(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s := openStore(t, b)
		var logs []*Log
		for i := range 3 {
			l := mustCreate(t, s, id)
			for j := range i + 2 {
				mustAppend(t, l, []byte{byte(j)})
			}
			l.Delete(0)
			l.Close()
			logs = append(logs, l)
		}
		victim := logs[1].ID()
		before := snapshot(t, b)
		if err := s.Delete(victim); err != nil {
			t.Fatal(err)
		}
		checkOnlyLogRemoved(t, before, snapshot(t, b), victim)
		if err := s.Delete(victim); err != nil {
			t.Fatalf("second Delete: %v", err)
		}
		ids, err := s.List()
		if err != nil || len(ids) != 2 || slices.Contains(ids, victim) {
			t.Fatalf("List after Delete = %v, %v", ids, err)
		}
		if _, err := s.Open(id, victim); !errors.Is(err, kololog.ErrMissingHeader) {
			t.Fatalf("Open of a deleted log: %v, want ErrMissingHeader", err)
		}
		for _, l := range []*Log{logs[0], logs[2]} {
			r, err := s.Open(id, l.ID())
			if err != nil {
				t.Fatal(err)
			}
			r.Close()
		}
	})
}

func TestConcurrentUse(t *testing.T) {
	forEachBackend(t, testConcurrentUse)
}

func testConcurrentUse(t *testing.T, st storage) {
	id := testIdentity(t)
	s := openStore(t, st.open(t))
	l := mustCreate(t, s, id)
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 20 {
				i, err := l.Append([]byte("x"))
				if err != nil {
					t.Error(err)
					return
				}
				if _, err := l.Get(i); err != nil {
					t.Error(err)
				}
				l.Head()
				l.Len()
			}
		})
	}
	wg.Go(func() {
		for range 10 {
			s.List()
			s.Create(id)
		}
	})
	wg.Wait()
	if l.Len() != 80 {
		t.Fatalf("Len = %d, want 80", l.Len())
	}
	l.Close()
	r, err := s.Open(id, l.ID())
	if err != nil || r.Len() != 80 {
		t.Fatalf("Open after concurrent appends: %v", err)
	}
}

func TestOpenLogCopiedUnderAnotherID(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s := openStore(t, b)
		a := mustCreate(t, s, id)
		mustAppend(t, a, []byte("a"))
		a.Close()
		other := kololog.LogID{0xbb}
		for k, v := range snapshot(t, b) {
			if bytes.HasPrefix([]byte(k), layout.LogPrefix(a.ID())) {
				putRaw(t, b, append(layout.LogPrefix(other), k[layout.LogPrefixLen:]...), []byte(v))
			}
		}
		putRaw(t, b, layout.ListKey(other), nil)
		got, err := s.Open(id, other)
		if got != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open of a log copied under another ID = %v, %v, want ErrCorrupt", got, err)
		}
		l, err := s.Open(id, a.ID())
		if err != nil {
			t.Fatalf("Open of the original log after the failed Open: %v", err)
		}
		l.Close()
	})
}

func TestTwoStoresOneBackend(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s1, s2 := New(b), New(b)
		l := mustCreate(t, s1, id)
		if _, err := s2.Open(id, l.ID()); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("Open through a second store: %v, want ErrLogInUse", err)
		}
		if err := s2.Delete(l.ID()); !errors.Is(err, ErrLogInUse) {
			t.Fatalf("Delete through a second store: %v, want ErrLogInUse", err)
		}
		l.Close()
		r, err := s2.Open(id, l.ID())
		if err != nil {
			t.Fatalf("Open through a second store after Close: %v", err)
		}
		r.Close()
	})
}

func TestOpenRollback(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s := openStore(t, b)
		l := mustCreate(t, s, id)
		mustAppend(t, l, []byte("a"))
		mustAppend(t, l, []byte("b"))
		old := snapshot(t, b)
		mustAppend(t, l, []byte("c"))
		published := mustHead(t, l)
		l.Close()

		putRaw(t, b, layout.HeadKey(l.ID()), []byte(old[string(layout.HeadKey(l.ID()))]))
		delRaw(t, b, layout.EntryKey(l.ID(), 2))
		delRaw(t, b, layout.KeyKey(l.ID(), 2))
		got, err := s.Open(id, l.ID(), kololog.WithTrustedHead(published))
		if got != nil || !errors.Is(err, kololog.ErrRollback) {
			t.Fatalf("Open of an older snapshot with the published head = %v, %v, want ErrRollback", got, err)
		}
	})
}

// deletedLog stores a log with entries 0 to 2, of which 1 and 2 are deleted, and returns its ID.
func deletedLog(t testing.TB, s *Store, id *koloid.Identity) kololog.LogID {
	t.Helper()
	l := mustCreate(t, s, id)
	for _, v := range []string{"a", "b", "c"} {
		mustAppend(t, l, []byte(v))
	}
	for _, i := range []uint64{1, 2} {
		if err := l.Delete(i); err != nil {
			t.Fatal(err)
		}
	}
	l.Close()
	return l.ID()
}

// storedEntry returns entry i as stored in b, with its key from its own backend key.
func storedEntry(t testing.TB, b Backend, logID kololog.LogID, i uint64) kololog.Entry {
	t.Helper()
	e, err := decodeEntry(getRaw(t, b, layout.EntryKey(logID, i)))
	if err != nil {
		t.Fatal(err)
	}
	k, err := b.Get(layout.KeyKey(logID, i))
	switch {
	case err == nil:
		e.Key = k
	case !errors.Is(err, ErrNotFound):
		t.Fatal(err)
	}
	return e
}

// putStored stores e as entry i in b, its key under its own backend key, or removes that key when e has none.
func putStored(t testing.TB, b Backend, logID kololog.LogID, i uint64, e kololog.Entry) {
	t.Helper()
	putRaw(t, b, layout.EntryKey(logID, i), layout.EncodeEntry(e))
	if e.Key != nil {
		putRaw(t, b, layout.KeyKey(logID, i), e.Key)
	} else {
		delRaw(t, b, layout.KeyKey(logID, i))
	}
}

func TestDeletedEntryRestores(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s := openStore(t, b)
		logID := deletedLog(t, s, id)
		l, err := s.Open(id, logID)
		if err != nil {
			t.Fatalf("Open of a log with deleted entries: %v", err)
		}
		defer l.Close()
		if _, err := l.Get(1); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("Get of a deleted entry: %v, want ErrDeleted", err)
		}
		e := storedEntry(t, b, logID, 1)
		if e.Ciphertext != nil || e.Marker == nil {
			t.Fatal("a deleted entry is stored with its ciphertext or without its marker")
		}
	})
}

func TestOpenTamperedDeletion(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t testing.TB, b Backend, logID, other kololog.LogID)
		want   error
	}{
		{"deleted flag without marker", func(t testing.TB, b Backend, logID, _ kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			var enc kolocodec.Encoder
			enc.PutUvarint(1)
			enc.PutBytes(e.Record)
			enc.PutBool(true)
			enc.PutBytes(nil)
			putRaw(t, b, layout.EntryKey(logID, 1), enc.Bytes())
		}, ErrCorrupt},
		{"forged marker", func(t testing.TB, b Backend, logID, _ kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			e.Marker = flipLast(e.Marker)
			putStored(t, b, logID, 1, e)
		}, kololog.ErrRestoreMismatch},
		{"marker for another index", func(t testing.TB, b Backend, logID, _ kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			e.Marker = storedEntry(t, b, logID, 2).Marker
			putStored(t, b, logID, 1, e)
		}, kololog.ErrRestoreMismatch},
		{"marker for another log", func(t testing.TB, b Backend, logID, other kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			e.Marker = storedEntry(t, b, other, 1).Marker
			putStored(t, b, logID, 1, e)
		}, kololog.ErrRestoreMismatch},
		{"version 0 value", func(t testing.TB, b Backend, logID, _ kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			var enc kolocodec.Encoder
			enc.PutUvarint(0)
			enc.PutBytes(e.Record)
			enc.PutBool(false)
			enc.PutBytes(nil)
			putRaw(t, b, layout.EntryKey(logID, 1), enc.Bytes())
		}, ErrCorrupt},
	}
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				b := failing{st.open(t), new(atomic.Bool)}
				s := New(b)
				defer s.Close()
				logID, other := deletedLog(t, s, id), deletedLog(t, s, id)
				if l, err := s.Open(id, logID); err != nil {
					t.Fatalf("Open before the damage: %v", err)
				} else {
					l.Close()
				}
				c.damage(t, b, logID, other)
				got, err := s.Open(id, logID)
				if got != nil || !errors.Is(err, ErrCorrupt) || !errors.Is(err, c.want) {
					t.Fatalf("Open = %v, %v, want no log, ErrCorrupt and %v", got, err, c.want)
				}
			})
		}
	})
}

func TestDeleteCrashKeepsCiphertext(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		fail := new(atomic.Bool)
		b := st.open(t)
		s := openStore(t, failing{b, fail})
		l := mustCreate(t, s, id)
		mustAppend(t, l, []byte("a"))
		fail.Store(true)
		if err := l.Delete(0); !errors.Is(err, errInjected) {
			t.Fatalf("Delete with a failing commit: %v", err)
		}
		fail.Store(false)
		l.Close()
		e := storedEntry(t, b, l.ID(), 0)
		if e.Ciphertext == nil || e.Marker != nil {
			t.Fatal("a failed Delete changed the stored entry")
		}
		r, err := s.Open(id, l.ID())
		if err != nil {
			t.Fatalf("Open after a failed Delete: %v", err)
		}
		defer r.Close()
		if got, err := r.Get(0); err != nil || string(got) != "a" {
			t.Fatalf("Get after a failed Delete: %q, %v, want a", got, err)
		}
	})
}

// countingScan counts the pairs every Scan hands to its callback.
type countingScan struct {
	Backend
	n *atomic.Int64
}

func (c countingScan) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	return c.Backend.Scan(start, end, reverse, limit, func(k, v []byte) bool {
		c.n.Add(1)
		return fn(k, v)
	})
}

// Open verifies stored entries while it reads them, and stops reading soon after the first bad one.
func TestOpenStopsAtBadEntry(t *testing.T) {
	cases := []struct {
		name   string
		damage func(t testing.TB, b Backend, logID kololog.LogID)
	}{
		{"ciphertext larger than its record", func(t testing.TB, b Backend, logID kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			e.Ciphertext = append(e.Ciphertext, make([]byte, 1<<20)...)
			putStored(t, b, logID, 1, e)
		}},
		{"forged record claiming a huge size", func(t testing.TB, b Backend, logID kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			r, err := kololog.DecodeRecord(e.Record)
			if err != nil {
				t.Fatal(err)
			}
			r.Size = 1 << 20
			e.Record, e.Ciphertext = r.Encode(), make([]byte, r.Size)
			putStored(t, b, logID, 1, e)
		}},
		{"record of another index", func(t testing.TB, b Backend, logID kololog.LogID) {
			putRaw(t, b, layout.EntryKey(logID, 1), getRaw(t, b, layout.EntryKey(logID, 0)))
		}},
		{"ciphertext with another hash", func(t testing.TB, b Backend, logID kololog.LogID) {
			e := storedEntry(t, b, logID, 1)
			flipLast(e.Ciphertext)
			putStored(t, b, logID, 1, e)
		}},
	}
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		for _, c := range cases {
			t.Run(c.name, func(t *testing.T) {
				n := new(atomic.Int64)
				b := countingScan{st.open(t), n}
				s := New(b)
				defer s.Close()
				l := mustCreate(t, s, id)
				for i := 0; i < 100; i++ {
					mustAppend(t, l, []byte{byte(i)})
				}
				l.Close()
				c.damage(t, b, l.ID())
				n.Store(0)
				got, err := s.Open(id, l.ID())
				if got != nil || !errors.Is(err, ErrCorrupt) || !errors.Is(err, kololog.ErrRestoreMismatch) {
					t.Fatalf("Open = %v, %v, want ErrCorrupt wrapping ErrRestoreMismatch", got, err)
				}
				// kolo-log reads ahead of the failing entry by at most 2*16+1 entries; entries 0 and 1 and
				// their keys are read side by side. A log of 100 entries read to the end would be far above this.
				if limit := int64(2 * (2 + 2*16 + 1)); n.Load() > limit {
					t.Fatalf("Open read %d pairs, want at most %d: it must stop near the bad entry 1", n.Load(), limit)
				}
			})
		}
	})
}

func TestOpenBoundedScanPastHead(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		n := new(atomic.Int64)
		b := countingScan{st.open(t), n}
		s := New(b)
		defer s.Close()
		l := mustCreate(t, s, id)
		for _, v := range []string{"a", "b", "c"} {
			mustAppend(t, l, []byte(v))
		}
		l.Close()
		extra := getRaw(t, b, layout.EntryKey(l.ID(), 2))
		batch := b.NewBatch()
		for i := uint64(3); i < 200; i++ {
			batch.Put(layout.EntryKey(l.ID(), i), extra)
		}
		if err := batch.Commit(); err != nil {
			t.Fatal(err)
		}
		n.Store(0)
		if got, err := s.Open(id, l.ID()); got != nil || !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Open with extra entries = %v, %v, want ErrCorrupt", got, err)
		}
		if n.Load() > 8 {
			t.Fatalf("Open read %d pairs for a head of size 3, want a bounded scan", n.Load())
		}
	})
}

func TestStoreCloseReleasesLogs(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		s := New(st.open(t))
		l := mustCreate(t, s, id)
		mustAppend(t, l, []byte("a"))
		if err := s.Close(); err != nil {
			t.Fatal(err)
		}
		s = openStore(t, st.open(t))
		r, err := s.Open(id, l.ID())
		if err != nil {
			t.Fatalf("Open after Store.Close left the log open: %v", err)
		}
		r.Close()
		if _, err := l.Get(0); !errors.Is(err, ErrClosed) {
			t.Fatalf("Get on a log of a closed store: %v, want ErrClosed", err)
		}
	})
}

// nonComparable is a backend type that cannot be a map key, so each Store on it keeps its own open logs.
type nonComparable struct {
	Backend
	_ []int
}

func TestDeleteLogOpenInAnotherStore(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s1, s2 := New(nonComparable{Backend: b}), New(nonComparable{Backend: b})
		l := mustCreate(t, s1, id)
		mustAppend(t, l, []byte("a"))
		err := s2.Delete(l.ID())
		if !errors.Is(err, ErrLogInUse) || !errors.Is(err, kololog.ErrLogOpen) {
			t.Fatalf("Delete of a log open in another store: %v, want ErrLogInUse wrapping kololog.ErrLogOpen", err)
		}
		if _, err := b.Get(layout.HeaderKey(l.ID())); err != nil {
			t.Fatalf("the open log lost its header: %v", err)
		}
		l.Close()
		if err := s2.Delete(l.ID()); err != nil {
			t.Fatalf("Delete after Close: %v", err)
		}
	})
}

var errCloseFailed = errors.New("close failed")

// blockingClose is a backend whose Close waits for release and then fails.
type blockingClose struct {
	Backend
	entered, release chan struct{}
}

func (b blockingClose) Close() error {
	close(b.entered)
	<-b.release
	b.Backend.Close()
	return errCloseFailed
}

func TestConcurrentStoreClose(t *testing.T) {
	b := blockingClose{NewMemory(), make(chan struct{}), make(chan struct{})}
	s := New(b)
	first := make(chan error, 1)
	go func() { first <- s.Close() }()
	<-b.entered
	second := make(chan error, 1)
	go func() { second <- s.Close() }()
	select {
	case err := <-second:
		t.Fatalf("second Close returned %v before the first finished", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(b.release)
	if err := <-first; !errors.Is(err, errCloseFailed) {
		t.Fatalf("first Close: %v", err)
	}
	if err := <-second; !errors.Is(err, errCloseFailed) {
		t.Fatalf("second Close: %v, want the first Close's error", err)
	}
}

// blockScan blocks the first Scan that starts at start until release is closed.
type blockScan struct {
	Backend
	start            []byte
	entered, release chan struct{}
	once             *sync.Once
}

func (b blockScan) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	if bytes.Equal(start, b.start) {
		b.once.Do(func() {
			close(b.entered)
			<-b.release
		})
	}
	return b.Backend.Scan(start, end, reverse, limit, fn)
}

func TestDeleteDuringOpen(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s2 := openStore(t, b)
		l := mustCreate(t, s2, id)
		mustAppend(t, l, []byte("a"))
		l.Close()
		blocked := blockScan{b, layout.EntryKey(l.ID(), 0), make(chan struct{}), make(chan struct{}), new(sync.Once)}
		s1 := New(blocked)
		opened := make(chan error, 1)
		go func() {
			r, err := s1.Open(id, l.ID())
			if err == nil {
				r.Close()
			}
			opened <- err
		}()
		<-blocked.entered
		err := s2.Delete(l.ID())
		close(blocked.release)
		if !errors.Is(err, ErrLogInUse) || !errors.Is(err, kololog.ErrLogOpen) {
			t.Errorf("Delete during Open: %v, want ErrLogInUse wrapping kololog.ErrLogOpen", err)
		}
		if err := <-opened; err != nil {
			t.Fatalf("Open after the concurrent Delete: %v", err)
		}
	})
}

type panickingClose struct{ Backend }

func (panickingClose) Close() error { panic("backend close") }

func TestStoreNilBackend(t *testing.T) {
	id := testIdentity(t)
	for name, b := range map[string]Backend{"nil": nil, "typed nil": (*memory)(nil)} {
		t.Run(name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Fatalf("store on a %s backend panicked: %v", name, r)
				}
			}()
			s := New(b)
			if _, err := s.List(); err == nil {
				t.Error("List succeeded")
			}
			if _, err := s.Create(id); err == nil {
				t.Error("Create succeeded")
			}
			if _, err := s.Open(id, kololog.LogID{1}); err == nil {
				t.Error("Open succeeded")
			}
			if err := s.Delete(kololog.LogID{1}); err == nil {
				t.Error("Delete succeeded")
			}
			if err := s.Close(); err != nil {
				t.Errorf("Close = %v, want nil", err)
			}
		})
	}
}

func TestStoreClosePanickingBackend(t *testing.T) {
	s := New(panickingClose{NewMemory()})
	var first error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Close panicked: %v", r)
			}
		}()
		first = s.Close()
	}()
	if first == nil {
		t.Fatal("Close of a panicking backend returned nil")
	}
	if second := s.Close(); second != first {
		t.Fatalf("second Close = %v, want the first error %v", second, first)
	}
}

func TestDeleteBetweenScanAndRestore(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		s1, s2 := New(nonComparable{Backend: b}), New(nonComparable{Backend: b})
		l := mustCreate(t, s2, id)
		mustAppend(t, l, []byte("a"))
		l.Close()
		var deleteErr error
		beforeRestore.set(func() { deleteErr = s2.Delete(l.ID()) })
		defer beforeRestore.set(nil)
		r, err := s1.Open(id, l.ID())
		if err != nil {
			t.Fatalf("Open: %v", err)
		}
		r.Close()
		if !errors.Is(deleteErr, ErrLogInUse) || !errors.Is(deleteErr, kololog.ErrLogOpen) {
			t.Fatalf("Delete between the scan and Restore: %v, want ErrLogInUse wrapping kololog.ErrLogOpen", deleteErr)
		}
		if _, err := b.Get(layout.HeaderKey(l.ID())); err != nil {
			t.Fatalf("the log was deleted: %v", err)
		}
	})
}

// closeTracker records that its Close ran.
type closeTracker struct {
	Backend
	closed *atomic.Bool
}

func (c closeTracker) Close() error {
	c.closed.Store(true)
	return c.Backend.Close()
}

func TestStoreCloseLogPanics(t *testing.T) {
	id := testIdentity(t)
	closed := new(atomic.Bool)
	s := New(closeTracker{NewMemory(), closed})
	var logs []*Log
	for range 3 {
		logs = append(logs, mustCreate(t, s, id))
	}
	var once sync.Once
	logClosing.set(func(*Log) { once.Do(func() { panic("log close") }) })
	defer logClosing.set(nil)
	var err error
	func() {
		defer func() {
			if r := recover(); r != nil {
				t.Fatalf("Store.Close panicked: %v", r)
			}
		}()
		err = s.Close()
	}()
	if err == nil || strings.Contains(err.Error(), "backend Close panicked") {
		t.Fatalf("Close with a panicking Log = %v, want an error that names the log", err)
	}
	if !closed.Load() {
		t.Error("the backend was not closed")
	}
	for i, l := range logs {
		if _, err := l.Append([]byte("x")); !errors.Is(err, ErrClosed) {
			t.Errorf("log %d after Store.Close: Append = %v, want ErrClosed", i, err)
		}
	}
}

func TestReopenAfterPanickingBackendClose(t *testing.T) {
	id := testIdentity(t)
	m := NewMemory()
	s := New(panickingClose{m})
	l := mustCreate(t, s, id)
	mustAppend(t, l, []byte("a"))
	if err := s.Close(); err == nil {
		t.Fatal("Close of a panicking backend returned nil")
	}
	r, err := New(m).Open(id, l.ID())
	if err != nil {
		t.Fatalf("Open on a new Store after a panicking backend Close: %v", err)
	}
	r.Close()
}

func TestDeleteWithUntrustedHeader(t *testing.T) {
	other, err := koloid.FromSeed(bytes.Repeat([]byte{9}, 32))
	if err != nil {
		t.Fatal(err)
	}
	cases := map[string]func(logID kololog.LogID) []byte{
		"undecodable header": func(kololog.LogID) []byte { return []byte{0xff} },
		"header naming another key": func(logID kololog.LogID) []byte {
			h := kololog.LogHeader{LogKey: other.PublicKey(), LogID: logID, Signature: make([]byte, 64)}
			return h.Encode()
		},
	}
	forEachBackend(t, func(t *testing.T, st storage) {
		id := testIdentity(t)
		b := st.open(t)
		for name, header := range cases {
			t.Run(name, func(t *testing.T) {
				s1, s2 := New(nonComparable{Backend: b}), New(nonComparable{Backend: b})
				l := mustCreate(t, s1, id)
				defer l.Close()
				putRaw(t, b, layout.HeaderKey(l.ID()), header(l.ID()))
				if err := s2.Delete(l.ID()); !errors.Is(err, ErrLogInUse) || !errors.Is(err, kololog.ErrLogOpen) {
					t.Fatalf("Delete of an open log with an untrusted header: %v, want ErrLogInUse wrapping kololog.ErrLogOpen", err)
				}
				if _, err := b.Get(layout.HeadKey(l.ID())); err != nil {
					t.Fatalf("the open log lost its head: %v", err)
				}
			})
		}
	})
}
