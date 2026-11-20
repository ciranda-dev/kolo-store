package layout

import (
	"bytes"
	"math"
	"slices"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
)

var orderIndexes = []uint64{
	0, 1, 255, 256, 65535, 65536,
	1<<32 - 1, 1 << 32, 1<<32 + 1,
	1<<53 - 1, 1 << 53, 1<<53 + 1,
	1 << 63, math.MaxUint64,
}

func TestEntryKeyOrder(t *testing.T) {
	var id kololog.LogID
	id[0] = 0xab
	for i := 1; i < len(orderIndexes); i++ {
		a, b := EntryKey(id, orderIndexes[i-1]), EntryKey(id, orderIndexes[i])
		if bytes.Compare(a, b) >= 0 {
			t.Errorf("EntryKey(%d) >= EntryKey(%d)", orderIndexes[i-1], orderIndexes[i])
		}
		if len(b) != EntryKeyLen || !bytes.HasPrefix(b, LogPrefix(id)) {
			t.Errorf("EntryKey(%d) = %x", orderIndexes[i], b)
		}
	}
	end := LogPrefixEnd(id)
	for _, k := range [][]byte{HeaderKey(id), HeadKey(id), EntryKey(id, math.MaxUint64)} {
		if bytes.Compare(k, LogPrefix(id)) < 0 || bytes.Compare(k, end) >= 0 {
			t.Errorf("key %x is outside the log prefix range", k)
		}
	}
}

func TestEntryValueRoundTrip(t *testing.T) {
	for _, e := range []kololog.Entry{
		{Record: []byte("record"), Ciphertext: []byte("ciphertext")},
		{Record: []byte("record"), Marker: []byte("marker")},
	} {
		got, err := DecodeEntry(EncodeEntry(e))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(got.Record, e.Record) || !bytes.Equal(got.Ciphertext, e.Ciphertext) || !bytes.Equal(got.Marker, e.Marker) ||
			(got.Ciphertext == nil) != (e.Ciphertext == nil) || (got.Marker == nil) != (e.Marker == nil) || got.Key != nil {
			t.Errorf("round trip of %+v gave %+v", e, got)
		}
	}
	keyed := EncodeEntry(kololog.Entry{Record: []byte("r"), Ciphertext: []byte("c"), Key: bytes.Repeat([]byte{9}, KeySize)})
	if !bytes.Equal(keyed, EncodeEntry(kololog.Entry{Record: []byte("r"), Ciphertext: []byte("c")})) {
		t.Error("EncodeEntry wrote the key into the entry value")
	}
	good := EncodeEntry(kololog.Entry{Record: []byte("r"), Ciphertext: []byte("c")})
	for name, b := range map[string][]byte{
		"empty":                  nil,
		"version 0":              append([]byte{0}, good[1:]...),
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
		if _, err := DecodeEntry(b); err == nil {
			t.Errorf("%s: DecodeEntry succeeded", name)
		}
	}
}

func TestKeyValue(t *testing.T) {
	k := bytes.Repeat([]byte{7}, KeySize)
	got, err := DecodeKey(k)
	if err != nil || !bytes.Equal(got, k) {
		t.Fatalf("DecodeKey = %x, %v", got, err)
	}
	k[0] = 8
	if got[0] != 7 {
		t.Fatal("DecodeKey did not copy")
	}
	for _, n := range []int{0, KeySize - 1, KeySize + 1} {
		if _, err := DecodeKey(make([]byte, n)); err == nil {
			t.Errorf("DecodeKey of %d bytes succeeded", n)
		}
	}
	var id kololog.LogID
	for i := 1; i < len(orderIndexes); i++ {
		if bytes.Compare(KeyKey(id, orderIndexes[i-1]), KeyKey(id, orderIndexes[i])) >= 0 {
			t.Errorf("KeyKey(%d) >= KeyKey(%d)", orderIndexes[i-1], orderIndexes[i])
		}
	}
	if bytes.Compare(KeyKey(id, 0), HeaderKey(id)) <= 0 || bytes.Compare(KeyKey(id, math.MaxUint64), HeadKey(id)) >= 0 {
		t.Error("entry keys do not sort between the header and the head")
	}
}

func TestTargetValue(t *testing.T) {
	head, proof, err := DecodeTarget(EncodeTarget([]byte("h"), []byte("p")))
	if err != nil || string(head) != "h" || string(proof) != "p" {
		t.Fatalf("round trip: %q, %q, %v", head, proof, err)
	}
	good := EncodeTarget([]byte("h"), []byte("p"))
	for name, b := range map[string][]byte{
		"empty":      nil,
		"cut":        good[:len(good)-1],
		"trailing":   append(slices.Clone(good), 0),
		"huge head":  {0xff, 0xff, 0x03},
		"huge proof": {1, 'h', 0xff, 0xff, 0x03},
	} {
		if _, _, err := DecodeTarget(b); err == nil {
			t.Errorf("%s: DecodeTarget succeeded", name)
		}
	}
}
