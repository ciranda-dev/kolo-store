package kolostore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"math/rand/v2"
	"slices"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

var orderIndexes = []uint64{
	0, 1, 255, 256, 65535, 65536,
	1<<32 - 1, 1 << 32, 1<<32 + 1,
	1<<53 - 1, 1 << 53, 1<<53 + 1,
	1 << 63, math.MaxUint64,
}

func TestEntryKeyScanOrder(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		var id kololog.LogID
		id[31] = 7
		shuffled := slices.Clone(orderIndexes)
		rand.Shuffle(len(shuffled), func(i, j int) { shuffled[i], shuffled[j] = shuffled[j], shuffled[i] })
		for _, i := range shuffled {
			if err := b.Put(layout.EntryKey(id, i), nil); err != nil {
				t.Fatal(err)
			}
		}
		for _, reverse := range []bool{false, true} {
			var got []uint64
			err := b.Scan(layout.LogPrefix(id), layout.LogPrefixEnd(id), reverse, 0, func(k, _ []byte) bool {
				got = append(got, binary.BigEndian.Uint64(k[layout.LogPrefixLen+1:]))
				return true
			})
			if err != nil {
				t.Fatal(err)
			}
			want := slices.Clone(orderIndexes)
			if reverse {
				slices.Reverse(want)
			}
			if !slices.Equal(got, want) {
				t.Errorf("reverse=%v: scan order %v, want %v", reverse, got, want)
			}
		}
	})
}

func TestDeleteEdgeLogIDs(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		var low, high, next kololog.LogID
		for i := range high {
			high[i] = 0xff
		}
		next[31] = 1
		for _, id := range []kololog.LogID{low, high, next} {
			for _, k := range [][]byte{layout.HeaderKey(id), layout.HeadKey(id), layout.EntryKey(id, 0), layout.EntryKey(id, math.MaxUint64)} {
				if err := b.Put(k, []byte("v")); err != nil {
					t.Fatal(err)
				}
			}
			if err := b.Put(layout.ListKey(id), nil); err != nil {
				t.Fatal(err)
			}
		}
		s := New(b)
		for _, id := range []kololog.LogID{high, low} {
			before := snapshot(t, b)
			if err := s.Delete(id); err != nil {
				t.Fatal(err)
			}
			checkOnlyLogRemoved(t, before, snapshot(t, b), id)
		}
		ids, err := s.List()
		if err != nil || len(ids) != 1 || ids[0] != next {
			t.Fatalf("List = %v, %v, want only %s", ids, err, next)
		}
	})
}

func TestListCorrupt(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		if err := b.Put([]byte{layout.PrefixList, 1}, nil); err != nil {
			t.Fatal(err)
		}
		if _, err := New(b).List(); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List with a short key: %v, want ErrCorrupt", err)
		}
	})
}

func TestDecodeEntryWrapsErrCorrupt(t *testing.T) {
	good := layout.EncodeEntry(kololog.Entry{Record: []byte("r"), Ciphertext: []byte("c")})
	for name, b := range map[string][]byte{
		"empty":                  nil,
		"version 0":              append([]byte{0}, good[1:]...),
		"version 2":              append([]byte{2}, good[1:]...),
		"version 4":              append([]byte{4}, good[1:]...),
		"cut":                    good[:len(good)-1],
		"trailing":               append(slices.Clone(good), 0),
		"no ciphertext":          {3, 1, 'r', 0, 0},
		"deleted without marker": {3, 1, 'r', 1, 0},
		"bad bool":               {3, 1, 'r', 2, 1, 'c'},
		"non-minimal version":    {0x83, 0x00, 1, 'r', 0, 1, 'c'},
		"huge record":            {3, 0xff, 0xff, 0x03},
		"huge marker":            {3, 1, 'r', 1, 0xff, 0xff, 0x03},
	} {
		if _, err := decodeEntry(b); !errors.Is(err, ErrCorrupt) {
			t.Errorf("%s: decodeEntry = %v, want ErrCorrupt", name, err)
		}
	}
}

// snapshot returns every key and value in b.
func snapshot(t testing.TB, b Backend) map[string]string {
	t.Helper()
	m := make(map[string]string)
	if err := b.Scan(nil, nil, false, 0, func(k, v []byte) bool {
		m[string(k)] = string(v)
		return true
	}); err != nil {
		t.Fatal(err)
	}
	return m
}

// checkOnlyLogRemoved fails unless after is before without the keys of log id.
func checkOnlyLogRemoved(t testing.TB, before, after map[string]string, id kololog.LogID) {
	t.Helper()
	want := make(map[string]string)
	for k, v := range before {
		if !bytes.HasPrefix([]byte(k), layout.LogPrefix(id)) && k != string(layout.ListKey(id)) {
			want[k] = v
		}
	}
	if len(after) != len(want) {
		t.Fatalf("after deleting log %s: %d keys, want %d", id, len(after), len(want))
	}
	for k, v := range want {
		if got, ok := after[k]; !ok || got != v {
			t.Fatalf("after deleting log %s: key %x changed", id, k)
		}
	}
}

func edgeIDs() (allFF, oneFF kololog.LogID) {
	for i := range allFF {
		allFF[i] = 0xff
		oneFF[i] = 0xab
	}
	oneFF[31] = 0xff
	return allFF, oneFF
}

// strayKey is the shortest key after layout.LogPrefix(id) that does not start with it.
func strayKey(id kololog.LogID) []byte {
	p := layout.LogPrefix(id)
	for len(p) > 0 && p[len(p)-1] == 0xff {
		p = p[:len(p)-1]
	}
	p[len(p)-1]++
	return p
}

func TestStrayKeyAfterLogPrefix(t *testing.T) {
	allFF, oneFF := edgeIDs()
	if got := strayKey(allFF); !bytes.Equal(got, []byte{layout.PrefixList}) {
		t.Fatalf("strayKey(allFF) = %x", got)
	}
	if got := strayKey(oneFF); len(got) != layout.LogPrefixLen-1 {
		t.Fatalf("strayKey(oneFF) = %x", got)
	}
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		for _, id := range []kololog.LogID{allFF, oneFF} {
			stray := strayKey(id)
			putRaw(t, b, stray, []byte("stray"))
			s := New(b)
			if _, err := s.Open(testIdentity(t), id); !errors.Is(err, kololog.ErrMissingHeader) {
				t.Fatalf("Open(%s) with stray key %x: %v, want ErrMissingHeader", id, stray, err)
			}
			if err := s.Delete(id); err != nil {
				t.Fatal(err)
			}
			if v, err := b.Get(stray); err != nil || string(v) != "stray" {
				t.Fatalf("Delete(%s) removed stray key %x: %v", id, stray, err)
			}
			b.Delete(stray)
		}
	})
}

// foreignScan returns key first from every Scan, as a broken backend might.
type foreignScan struct {
	Backend
	key []byte
}

func (f foreignScan) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	if !fn(clone(f.key), nil) {
		return nil
	}
	return f.Backend.Scan(start, end, reverse, limit, fn)
}

func TestOpenRejectsKeysOutsideThePrefix(t *testing.T) {
	m := NewMemory()
	l := mustCreate(t, New(m), testIdentity(t))
	mustAppend(t, l, []byte("a"))
	l.Close()
	for _, k := range [][]byte{{layout.PrefixList}, layout.LogPrefix(l.ID())[:layout.LogPrefixLen-1], layout.LogPrefix(kololog.LogID{0xce})} {
		s := New(foreignScan{m, k})
		if _, err := s.Open(testIdentity(t), l.ID()); !errors.Is(err, ErrCorrupt) {
			t.Errorf("Open with scanned key %x: %v, want ErrCorrupt", k, err)
		}
	}
}
