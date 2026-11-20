package layout

import (
	"bytes"
	"testing"

	kololog "github.com/ciranda-dev/kolo-log"
)

func FuzzDecodeEntry(f *testing.F) {
	f.Add(EncodeEntry(kololog.Entry{Record: []byte("record"), Ciphertext: []byte("ciphertext")}))
	f.Add(EncodeEntry(kololog.Entry{Record: []byte("record"), Marker: []byte("marker")}))
	f.Add([]byte{})
	f.Add([]byte{1, 0, 1, 0})
	f.Add(append([]byte{2, 1, 'r', 0, 1, 'c', 32}, make([]byte, 32)...))
	f.Fuzz(func(t *testing.T, b []byte) {
		e, err := DecodeEntry(b)
		if err != nil {
			return
		}
		enc := EncodeEntry(e)
		if !bytes.Equal(enc, b) {
			t.Fatalf("decoded entry encodes to other bytes")
		}
		again, err := DecodeEntry(enc)
		if err != nil || !bytes.Equal(again.Record, e.Record) || !bytes.Equal(again.Ciphertext, e.Ciphertext) ||
			!bytes.Equal(again.Marker, e.Marker) || again.Key != nil {
			t.Fatalf("re-encoded entry decodes to another entry: %v", err)
		}
	})
}

func FuzzDecodeTarget(f *testing.F) {
	f.Add(EncodeTarget([]byte("head"), []byte("proof")))
	f.Add([]byte{})
	f.Add([]byte{0xff, 0xff, 0x03})
	f.Fuzz(func(t *testing.T, b []byte) {
		head, proof, err := DecodeTarget(b)
		if err != nil {
			return
		}
		if !bytes.Equal(EncodeTarget(head, proof), b) {
			t.Fatal("decoded target encodes to other bytes")
		}
	})
}
