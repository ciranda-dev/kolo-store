package kolostore

import (
	"bufio"
	"bytes"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"os"
	"slices"
	"strings"
	"sync/atomic"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

// keylessReplica stores on s a replica of a new log of n entries "0", "1", ... with the given ones deleted, every entry
// without its key, and returns it with the keys (nil for a deleted entry).
func keylessReplica(t testing.TB, s *Store, n int, deleted ...uint64) (*Replica, []kololog.EntryKey) {
	t.Helper()
	owner := newIdentity(t)
	w := mustCreate(t, openStore(t, NewMemory()), owner)
	for i := range n {
		mustAppend(t, w, []byte{'0' + byte(i)})
	}
	if len(deleted) > 0 {
		if err := w.Delete(deleted...); err != nil {
			t.Fatal(err)
		}
	}
	var entries []kololog.Entry
	var keys []kololog.EntryKey
	for i := range uint64(n) {
		e, err := w.Entry(i)
		if err != nil {
			t.Fatal(err)
		}
		keys = append(keys, e.Key)
		e.Key = nil
		entries = append(entries, e)
	}
	r := mustOpenReplica(t, s, owner.PublicKey(), w.ID(), w.Header().Encode())
	if err := r.Update(mustHead(t, w), kololog.ConsistencyProof{}, entries); err != nil {
		t.Fatal(err)
	}
	return r, keys
}

func clones(keys ...kololog.EntryKey) []kololog.EntryKey {
	out := make([]kololog.EntryKey, len(keys))
	for i, k := range keys {
		out[i] = bytes.Clone(k)
	}
	return out
}

// TestStoredReplicaAddKeys stores the keys AddKeys accepts in one batch, keeps them after the caller changes its slices and
// after a reopen, and writes nothing for keys already stored.
func TestStoredReplicaAddKeys(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		s := New(b)
		r, keys := keylessReplica(t, s, 5, 4)
		writes := 0
		r.s.b = countingBatches{b, &writes}
		in := clones(keys[0], keys[1], keys[0], nil)
		var bad *kololog.BadKeyError
		if err := r.AddKeys(0, in); !errors.As(err, &bad) || !slices.Equal(bad.Indexes, []uint64{2}) || writes != 1 {
			t.Fatalf("AddKeys = %v, %d writes, want index 2 refused in 1 write", err, writes)
		}
		for _, k := range in {
			clear(k)
		}
		for i, want := range []kololog.EntryKey{keys[0], keys[1], nil, nil} {
			if got := storedEntry(t, b, r.ID(), uint64(i)).Key; !bytes.Equal(got, want) {
				t.Fatalf("stored key %d = %x, want %x", i, got, want)
			}
		}
		writes = 0
		if err := r.AddKeys(0, clones(keys...)); err != nil || writes != 1 {
			t.Fatalf("AddKeys of every key = %v, %d writes", err, writes)
		}
		writes = 0
		if err := r.AddKeys(1, clones(keys[1:]...)); err != nil || writes != 0 {
			t.Fatalf("AddKeys of stored keys = %v, %d writes", err, writes)
		}
		r.s.b = b
		pub := r.Header().LogKey
		r.Close()
		s.Close()

		s = openStore(t, st.open(t))
		r = mustOpenReplica(t, s, pub, r.ID(), nil)
		defer r.Close()
		for i := range uint64(4) {
			if got, err := r.Get(i); err != nil || string(got) != string('0'+byte(i)) {
				t.Fatalf("Get(%d) after reopen = %q, %v", i, got, err)
			}
			if k, err := r.Key(i); err != nil || !bytes.Equal(k, keys[i]) {
				t.Fatalf("Key(%d) after reopen = %v", i, err)
			}
		}
		if _, err := r.Get(4); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("Get(4) = %v, want ErrDeleted", err)
		}
	})
}

// TestAddKeysCrash crashes at the batch of AddKeys: after a reopen every key is stored, or none.
func TestAddKeysCrash(t *testing.T) {
	for _, k := range kinds {
		for _, after := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/applied=%v", k.name, after), func(t *testing.T) {
				st := k.new(t)
				s := New(st.open(t))
				r, keys := keylessReplica(t, s, 3)
				var n int
				r.s.b = crashAt{r.s.b, 1, after, &n, new(atomic.Bool)}
				// The batch is the last thing AddKeys does, so a crash just after it reads as success.
				if err := r.AddKeys(0, clones(keys...)); after != (err == nil) || (!after && !errors.Is(err, errInjected)) {
					t.Fatalf("AddKeys = %v", err)
				}
				pub := r.Header().LogKey
				r.Close()
				s.Close()

				s = openStore(t, st.open(t))
				r = mustOpenReplica(t, s, pub, r.ID(), nil)
				defer r.Close()
				for i := range uint64(3) {
					info, err := r.EntryInfo(i)
					if err != nil || info.Keyed != after {
						t.Fatalf("after a crash (applied %v) entry %d keyed %v, %v", after, i, info.Keyed, err)
					}
				}
			})
		}
	}
}

// TestReplicaAddKeysRefusals writes nothing for a refused call: too many keys, keys past the end, a closed replica, a failed
// write and an entry changed on disk.
func TestReplicaAddKeysRefusals(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		fail := new(atomic.Bool)
		s := New(failing{b, fail})
		defer s.Close()
		r, keys := keylessReplica(t, s, 2)
		before := snapshot(t, b)
		unchanged := func(name string, err, want error) {
			t.Helper()
			if !errors.Is(err, want) || !sameSnapshot(before, snapshot(t, b)) {
				t.Fatalf("%s: %v, want %v, or the disk changed", name, err, want)
			}
		}
		// kolo-log bounds one call at kololog.MaxAddKeys.
		unchanged("too many keys", r.AddKeys(0, make([]kololog.EntryKey, kololog.MaxAddKeys+1)), kololog.ErrTooManyKeys)
		unchanged("past the end", r.AddKeys(1, clones(keys...)), kololog.ErrOutOfRange)
		unchanged("start past the end", r.AddKeys(math.MaxUint64, clones(keys[0])), kololog.ErrOutOfRange)
		fail.Store(true)
		unchanged("failed write", r.AddKeys(0, clones(keys...)), errInjected)
		fail.Store(false)

		good := storedEntry(t, b, r.ID(), 1)
		bad := good
		bad.Ciphertext = flip(good.Ciphertext)
		putStored(t, b, r.ID(), 1, bad)
		before = snapshot(t, b)
		err := r.AddKeys(0, clones(keys...))
		unchanged("changed entry", err, kololog.ErrStorageMismatch)
		unchanged("changed entry", err, ErrCorrupt)
		putStored(t, b, r.ID(), 1, good)

		r.Close()
		before = snapshot(t, b)
		unchanged("closed replica", r.AddKeys(0, clones(keys...)), ErrClosed)
	})
}

// TestCommitKeys writes every key of a Change in its one batch, under its own index, from a copy of the caller's slice.
func TestCommitKeys(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		writes := 0
		s := openStore(t, countingBatches{b, &writes})
		ls := &logStorage{s: s, id: kololog.LogID{7}}
		key := func(i int) kololog.EntryKey { return bytes.Repeat([]byte{byte(i) + 1}, layout.KeySize) }
		keys := make([]kololog.KeyAt, kololog.MaxAddKeys)
		for i := range keys {
			keys[i] = kololog.KeyAt{Index: uint64(2 * i), Key: key(i)}
		}
		if err := ls.Commit(kololog.Change{Start: 2 * kololog.MaxAddKeys, Keys: keys}); err != nil || writes != 1 {
			t.Fatalf("Commit of %d keys = %v, %d writes", len(keys), err, writes)
		}
		for i, k := range keys {
			if !bytes.Equal(k.Key, key(i)) {
				t.Fatalf("Commit changed the caller's key %d", i)
			}
			clear(k.Key)
		}
		n := 0
		if err := b.Scan(layout.KeyKey(ls.id, 0), layout.KeyKey(ls.id, math.MaxUint64), false, 0, func(k, v []byte) bool {
			if !bytes.Equal(k, layout.KeyKey(ls.id, uint64(2*n))) || !bytes.Equal(v, key(n)) {
				t.Fatalf("stored key %d is %x under %x", n, v, k)
			}
			n++
			return true
		}); err != nil || n != len(keys) {
			t.Fatalf("%d keys stored, %v", n, err)
		}
	})
}

// TestCommitRefusesBadKeys refuses, writing nothing, a Change whose keys a log never sends: a repeated or unordered index,
// an index not stored before the change, or a key of the wrong size.
func TestCommitRefusesBadKeys(t *testing.T) {
	k := bytes.Repeat([]byte{1}, layout.KeySize)
	at := func(i uint64) kololog.KeyAt { return kololog.KeyAt{Index: i, Key: k} }
	entry := kololog.Entry{Record: []byte("r"), Ciphertext: []byte("c")}
	for _, tc := range []struct {
		name string
		c    kololog.Change
	}{
		{"duplicate", kololog.Change{Start: 3, Keys: []kololog.KeyAt{at(1), at(1)}}},
		{"descending", kololog.Change{Start: 3, Keys: []kololog.KeyAt{at(2), at(1)}}},
		{"at the end", kololog.Change{Start: 3, Keys: []kololog.KeyAt{at(0), at(3)}}},
		{"new entry", kololog.Change{Start: 3, Entries: []kololog.Entry{entry}, Keys: []kololog.KeyAt{at(3)}}},
		{"largest index", kololog.Change{Start: 3, Keys: []kololog.KeyAt{at(math.MaxUint64)}}},
		{"short key", kololog.Change{Start: 3, Keys: []kololog.KeyAt{{Index: 0, Key: k[:layout.KeySize-1]}}}},
		{"long key", kololog.Change{Start: 3, Keys: []kololog.KeyAt{{Index: 0, Key: append(k, 0)}}}},
		{"nil key", kololog.Change{Start: 3, Keys: []kololog.KeyAt{{Index: 0}}}},
		{"with entries", kololog.Change{Start: 2, Entries: []kololog.Entry{entry}, Keys: []kololog.KeyAt{at(0), at(0)}}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forEachBackend(t, func(t *testing.T, st storage) {
				b := st.open(t)
				s := openStore(t, b)
				ls := &logStorage{s: s, id: kololog.LogID{7}}
				if err := ls.Commit(tc.c); err == nil || !strings.HasPrefix(err.Error(), "kolostore: ") {
					t.Fatalf("Commit = %v, want a kolostore error", err)
				}
				if m := snapshot(t, b); len(m) != 0 {
					t.Fatalf("a refused Commit wrote %d keys", len(m))
				}
			})
		})
	}
	if !bytes.Equal(k, bytes.Repeat([]byte{1}, layout.KeySize)) {
		t.Fatal("Commit changed a refused key")
	}
}

// loadFixture puts the backend dump of testdata/name into b and returns the owner key and log ID it names.
func loadFixture(t testing.TB, b Backend, name string) (owner []byte, id kololog.LogID) {
	t.Helper()
	f, err := os.Open("testdata/" + name)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	sc.Buffer(nil, 1<<20)
	for sc.Scan() {
		k, v, ok := strings.Cut(sc.Text(), " ")
		if !ok || strings.HasPrefix(k, "#") {
			continue
		}
		vb, err := hex.DecodeString(v)
		if err != nil {
			t.Fatal(err)
		}
		switch k {
		case "owner":
			owner = vb
		case "log":
			copy(id[:], vb)
		default:
			kb, err := hex.DecodeString(k)
			if err != nil {
				t.Fatal(err)
			}
			putRaw(t, b, kb, vb)
		}
	}
	if err := sc.Err(); err != nil {
		t.Fatal(err)
	}
	return owner, id
}

// TestOpenStoredReplica opens a replica from a stored dump, reads its keys back, and adds keys to it.
func TestOpenStoredReplica(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		owner, id := loadFixture(t, b, "replica.txt")
		s := openStore(t, b)
		r := mustOpenReplica(t, s, owner, id, nil)
		defer r.Close()
		for i, v := range []string{"a", "b"} {
			if got, err := r.Get(uint64(i)); err != nil || string(got) != v {
				t.Fatalf("Get(%d) = %q, %v", i, got, err)
			}
		}
		if _, err := r.Get(2); !errors.Is(err, kololog.ErrKeyUnavailable) {
			t.Fatalf("Get(2) = %v, want ErrKeyUnavailable", err)
		}
		if _, err := r.Get(3); !errors.Is(err, kololog.ErrDeleted) {
			t.Fatalf("Get(3) = %v, want ErrDeleted", err)
		}
		k0, err := r.Key(0)
		if err != nil {
			t.Fatal(err)
		}
		k1, err := r.Key(1)
		if err != nil {
			t.Fatal(err)
		}
		var bad *kololog.BadKeyError
		if err := r.AddKeys(0, []kololog.EntryKey{k0, k1, k0}); !errors.As(err, &bad) || !slices.Equal(bad.Indexes, []uint64{2}) {
			t.Fatalf("AddKeys = %v, want index 2 refused", err)
		}
	})
}
