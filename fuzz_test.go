package kolostore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

type fuzzSlot struct {
	id      kololog.LogID
	exists  bool
	l       *Log // nil when closed
	enc     kololog.ValueEncoding
	head    kololog.Head // last head written
	deleted map[uint64]bool
}

func fuzzValue(enc kololog.ValueEncoding, b byte) any {
	switch enc {
	case kololog.UTF8:
		return string(rune('a' + b%26))
	case kololog.JSON:
		return map[string]int{"v": int(b)}
	}
	return []byte{b}
}

// FuzzOps runs random create, append, delete, reopen and delete-log steps on up to three logs.
func FuzzOps(f *testing.F) {
	id := testIdentity(f)
	f.Add([]byte{0, 0, 1, 0, 1, 0, 2, 0, 3, 0, 1, 0, 4, 0})
	f.Add([]byte{0, 0, 0, 1, 0, 5, 1, 0, 1, 1, 1, 2, 3, 1, 2, 2, 4, 0, 3, 2, 1, 2})
	f.Add([]byte{0, 2, 1, 2, 5, 2, 3, 2, 2, 2, 3, 2, 4, 2, 0, 2, 1, 2})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 128 {
			ops = ops[:128]
		}
		m := NewMemory()
		s := New(m)
		var slots [3]fuzzSlot
		for i := 0; i+1 < len(ops); i += 2 {
			op, arg := ops[i]%6, ops[i+1]
			sl := &slots[int(arg)%len(slots)]
			before := snapshot(t, m)
			switch {
			case op == 0 && !sl.exists:
				enc := kololog.ValueEncoding(arg / 3 % 3)
				l, err := s.Create(id, kololog.WithValueEncoding(enc))
				if err != nil {
					t.Fatal(err)
				}
				*sl = fuzzSlot{id: l.ID(), exists: true, l: l, enc: enc, head: mustHead(t, l), deleted: map[uint64]bool{}}
			case op == 1 && sl.l != nil:
				if _, err := sl.l.Append(fuzzValue(sl.enc, arg)); err != nil {
					t.Fatal(err)
				}
				sl.head = mustHead(t, sl.l)
			case op == 2 && sl.l != nil && sl.l.Len() > 0:
				// One index, or several with a repeat, in one batch.
				index, other := uint64(arg)%sl.l.Len(), uint64(arg/7)%sl.l.Len()
				indexes := []uint64{index}
				if arg%2 == 1 {
					indexes = append(indexes, other, index)
				}
				if err := sl.l.Delete(indexes...); err != nil {
					t.Fatal(err)
				}
				for _, i := range indexes {
					sl.deleted[i] = true
				}
			case op == 3 && sl.exists:
				if sl.l != nil {
					sl.l.Close()
				}
				l, err := s.Open(id, sl.id, kololog.WithValueEncoding(sl.enc))
				if err != nil {
					t.Fatalf("reopen: %v", err)
				}
				sl.l = l
			case op == 4 && sl.exists:
				if sl.l != nil {
					sl.l.Close()
				}
				if err := s.Delete(sl.id); err != nil {
					t.Fatal(err)
				}
				*sl = fuzzSlot{id: sl.id}
			case op == 5 && sl.l != nil:
				sl.l.Close()
				sl.l = nil
			default:
				continue
			}
			after := snapshot(t, m)
			for k, v := range before {
				if !bytes.HasPrefix([]byte(k), layout.LogPrefix(sl.id)) && k != string(layout.ListKey(sl.id)) && after[k] != v {
					t.Fatalf("step %d changed key %x of another log", i/2, k)
				}
			}
			checkStored(t, s, sl, after)
		}
		for _, sl := range slots {
			if sl.l != nil {
				sl.l.Close()
			}
		}
		s.Close()
	})
}

// checkStored compares an open log with its stored bytes, and restores a closed log from the backend.
func checkStored(t *testing.T, s *Store, sl *fuzzSlot, all map[string]string) {
	t.Helper()
	if !sl.exists {
		for k := range all {
			if bytes.HasPrefix([]byte(k), layout.LogPrefix(sl.id)) || k == string(layout.ListKey(sl.id)) {
				t.Fatalf("deleted log left key %x", k)
			}
		}
		return
	}
	if sl.l != nil {
		h, err := kololog.DecodeHead([]byte(all[string(layout.HeadKey(sl.id))]))
		if err != nil || !sameTree(h, sl.head) || !sameTree(mustHead(t, sl.l), sl.head) {
			t.Fatalf("stored head differs from the open log: %v", err)
		}
		n := uint64(0)
		for k := range all {
			if bytes.HasPrefix([]byte(k), layout.EntryKey(sl.id, 0)[:layout.LogPrefixLen+1]) {
				n++
			}
		}
		if n != sl.l.Len() {
			t.Fatalf("%d stored entries, open log has %d", n, sl.l.Len())
		}
		for i := range n {
			e, err := sl.l.log.Entry(i)
			if err != nil || all[string(layout.EntryKey(sl.id, i))] != string(layout.EncodeEntry(e)) || all[string(layout.KeyKey(sl.id, i))] != string(e.Key) {
				t.Fatalf("stored entry %d differs from the open log: %v", i, err)
			}
		}
		return
	}
	r, err := s.Open(testIdentity(t), sl.id, kololog.WithValueEncoding(sl.enc))
	if err != nil {
		t.Fatalf("stored log does not restore: %v", err)
	}
	defer r.Close()
	if !sameTree(mustHead(t, r), sl.head) || r.Len() != sl.head.Size {
		t.Fatalf("stored log has %d entries and another head, want size %d", r.Len(), sl.head.Size)
	}
	for i := range r.Len() {
		var want error
		if sl.deleted[i] {
			want = kololog.ErrDeleted
		}
		if _, err := r.Get(i); !errors.Is(err, want) {
			t.Fatalf("entry %d: Get = %v, want %v", i, err, want)
		}
	}
}

// appendPair appends one FuzzOpenBackend pair: mode, uvarint key length, key, uvarint value length, value.
func appendPair(b []byte, mode byte, key, value []byte) []byte {
	b = append(b, mode)
	b = binary.AppendUvarint(b, uint64(len(key)))
	b = append(b, key...)
	b = binary.AppendUvarint(b, uint64(len(value)))
	return append(b, value...)
}

// openBackendSeeds are the seed inputs of FuzzOpenBackend, among them a real store with live and deleted entries.
func openBackendSeeds(t testing.TB) [][]byte {
	_, oneFF := edgeIDs()
	m := NewMemory()
	s := New(m)
	l := mustCreate(t, s, testIdentity(t))
	for _, v := range []string{"a", "b", "c"} {
		mustAppend(t, l, []byte(v))
	}
	if err := l.Delete(1); err != nil {
		t.Fatal(err)
	}
	l.Close()
	var real []byte
	if err := m.Scan(nil, nil, false, 0, func(k, v []byte) bool {
		real = appendPair(real, 0, k, v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	w := mustCreate(t, New(NewMemory()), testIdentity(t))
	rm := NewMemory()
	r, err := New(rm).OpenReplica(testIdentity(t).PublicKey(), w.ID(), w.Header().Encode())
	if err != nil {
		t.Fatal(err)
	}
	for _, v := range []string{"a", "b", "c", "d"} {
		mustAppend(t, w, []byte(v))
	}
	if err := syncChunk(t, r, w, 2); err != nil {
		t.Fatal(err)
	}
	r.Close()
	w.Close()
	var replica []byte
	if err := rm.Scan(nil, nil, false, 0, func(k, v []byte) bool {
		replica = appendPair(replica, 0, k, v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return [][]byte{
		real,
		replica,
		appendPair(nil, 0, []byte{layout.PrefixList}, nil),
		appendPair(appendPair(nil, 1, []byte{layout.SuffixHeader}, []byte{1, 2, 3}), 1, []byte{layout.SuffixHead}, nil),
		appendPair(nil, 0, strayKey(oneFF), nil),
	}
}

func nextChunk(data []byte) (chunk, rest []byte) {
	n, k := binary.Uvarint(data)
	if k <= 0 {
		return nil, nil
	}
	data = data[k:]
	n = min(n, uint64(len(data)))
	return data[:n], data[n:]
}

// openBackendInput fills a memory backend from data and opens the logs in it; it must never panic.
func openBackendInput(t *testing.T, data []byte) {
	id := testIdentity(t)
	allFF, oneFF := edgeIDs()
	ids := []kololog.LogID{allFF, oneFF, {0xab}}
	b := NewMemory()
	for len(data) > 0 {
		mode := data[0]
		var key, value []byte
		key, data = nextChunk(data[1:])
		value, data = nextChunk(data)
		if mode%4 > 0 {
			key = append(layout.LogPrefix(ids[mode%4-1]), key...)
		}
		if err := b.Put(key, value); err != nil {
			t.Fatal(err)
		}
	}
	s := New(b)
	listed, _ := s.List()
	if len(listed) > 4 {
		listed = listed[:4]
	}
	for _, logID := range append(ids, listed...) {
		if l, err := s.Open(id, logID); err == nil {
			l.Close()
		}
		if r, err := s.OpenReplica(id.PublicKey(), logID, nil); err == nil {
			r.Close()
		}
	}
}

// FuzzOpenBackend fills a memory backend with arbitrary pairs, many near the edge log IDs; Open, OpenReplica and List must not panic.
func FuzzOpenBackend(f *testing.F) {
	for _, seed := range openBackendSeeds(f) {
		f.Add(seed)
	}
	f.Fuzz(openBackendInput)
}

func TestOpenBackendSeedsReachEntries(t *testing.T) {
	var live, deleted int
	entryDecoded.set(func(e kololog.Entry) {
		if e.Marker != nil {
			deleted++
		} else {
			live++
		}
	})
	defer entryDecoded.set(nil)
	for _, seed := range openBackendSeeds(t) {
		openBackendInput(t, seed)
	}
	if live == 0 || deleted == 0 {
		t.Fatalf("FuzzOpenBackend seeds decoded %d live and %d deleted entries, want both", live, deleted)
	}
}

// replicaState is everything a reader sees of a stored replica, to compare it across a reopen.
func replicaState(t testing.TB, r *Replica) string {
	t.Helper()
	var b bytes.Buffer
	if h, err := r.Head(); err == nil {
		b.Write(h.Encode())
	}
	if h, p, ok := r.Target(); ok {
		b.WriteString("|target")
		b.Write(h.Encode())
		b.Write(p.Encode())
	}
	for i := range r.Len() {
		e, err := r.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		got, err := r.Get(i)
		b.WriteString(fmt.Sprintf("|%x|%x|%x|%x|%q|%v", e.Record, e.Ciphertext, e.Marker, []byte(e.Key), got, err))
	}
	return b.String()
}

// FuzzReplicaOps runs random owner appends and deletes, chunks, full syncs, held heads, AddKey, AddKeys, ApplyDelete and reopens
// against a stored replica; every reopen must give back exactly the state before it.
func FuzzReplicaOps(f *testing.F) {
	f.Add([]byte{0, 0, 0, 1, 0, 2, 0, 3, 0, 4, 0, 5, 0, 6, 0, 7, 0, 8, 0, 9, 3, 19, 4, 4, 7, 0})
	f.Add([]byte{0, 0, 0, 1, 0, 2, 2, 1, 5, 0, 1, 1, 6, 1, 7, 1, 0, 3, 3, 0, 7, 0, 2, 0, 7, 0})
	f.Add([]byte{0, 0, 0, 1, 0, 2, 0, 3, 3, 4, 0, 4, 0, 5, 3, 1, 4, 5, 7, 0, 3, 3, 7, 1})
	f.Add([]byte{0, 0, 0, 1, 0, 2, 0, 3, 2, 1, 1, 3, 5, 1, 7, 1})
	f.Fuzz(func(t *testing.T, ops []byte) {
		if len(ops) > 96 {
			ops = ops[:96]
		}
		owner := testIdentity(t)
		ws := New(NewMemory())
		defer ws.Close()
		w := mustCreate(t, ws, owner)
		heads := []kololog.Head{mustHead(t, w)}
		m := NewMemory()
		s := New(keepOpen{m})
		r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
		reopen := func(store bool) {
			before := replicaState(t, r)
			r.Close()
			if store {
				s.Close()
				s = New(keepOpen{m})
			}
			var err error
			if r, err = s.OpenReplica(owner.PublicKey(), w.ID(), nil); err != nil {
				t.Fatalf("reopen: %v", err)
			}
			if after := replicaState(t, r); after != before {
				t.Fatalf("reopen changed the replica:\n%s\n%s", before, after)
			}
		}
		entries := func(from, to uint64, dropKeys bool) []kololog.Entry {
			var out []kololog.Entry
			for i := from; i < to; i++ {
				e, err := w.Entry(i)
				if err != nil {
					t.Fatal(err)
				}
				if dropKeys && i%2 == 1 {
					e.Key = nil
				}
				out = append(out, e)
			}
			return out
		}
		var err error
		for i := 0; i+1 < len(ops); i += 2 {
			op, arg := ops[i]%8, ops[i+1]
			switch op {
			case 0:
				mustAppend(t, w, []byte{arg})
				heads = append(heads, mustHead(t, w))
			case 1:
				if w.Len() > 0 {
					err = w.Delete(uint64(arg)%w.Len(), uint64(arg/5)%w.Len())
				}
			case 2:
				h := mustHead(t, w)
				var p kololog.ConsistencyProof
				if r.Len() <= h.Size {
					p, _ = w.ConsistencyProof(r.Len(), h.Size)
					err = r.Update(h, p, entries(r.Len(), h.Size, arg%2 == 1))
				}
			case 3:
				h := heads[int(arg/4)%len(heads)]
				to := min(r.Len()+1+uint64(arg%4), h.Size)
				if r.Len() < to {
					p, _ := w.ConsistencyProof(to, h.Size)
					err = r.Update(h, p, entries(r.Len(), to, arg%2 == 1))
				}
			case 4:
				err = r.Update(heads[int(arg)%len(heads)], kololog.ConsistencyProof{}, nil)
			case 5:
				// AddKey of one entry, or AddKeys of every entry from it on.
				if n := min(r.Len(), w.Len()); n > 0 {
					from := uint64(arg/2) % n
					if arg%2 == 0 {
						if e, _ := w.Entry(from); e.Key != nil {
							err = r.AddKey(from, e.Key)
						}
						break
					}
					var keys []kololog.EntryKey
					for i := from; i < n; i++ {
						e, _ := w.Entry(i)
						keys = append(keys, e.Key)
					}
					err = r.AddKeys(from, keys)
				}
			case 6:
				// The markers of up to three entries from arg on, in one batch.
				var ms [][]byte
				n := min(r.Len(), w.Len())
				for i := uint64(arg) % max(n, 1); i < uint64(arg)%max(n, 1)+3 && i < n; i++ {
					if e, _ := w.Entry(i); e.Marker != nil {
						ms = append(ms, e.Marker)
					}
				}
				err = r.ApplyDelete(ms...)
			case 7:
				reopen(arg%2 == 1)
			}
			if errors.Is(err, ErrCorrupt) {
				t.Fatalf("op %d: %v", op, err)
			}
		}
		reopen(true)
		r.Close()
		s.Close()
	})
}
