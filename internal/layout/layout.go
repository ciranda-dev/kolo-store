// Package layout defines the store's key layout and the binary encoding of stored entries.
package layout

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	kolocodec "github.com/ciranda-dev/kolo-codec"
	kololog "github.com/ciranda-dev/kolo-log"
)

// Key prefixes, log key suffixes and key sizes; widths are fixed so prefixes never collide.
const (
	PrefixLog  byte = 0x01
	PrefixList byte = 0x02

	SuffixHeader byte = 'h'
	SuffixHead   byte = 't'
	SuffixTarget byte = 'u'
	SuffixEntry  byte = 'e'
	SuffixKey    byte = 'k'

	LogPrefixLen = 1 + kololog.LogIDSize
	EntryKeyLen  = LogPrefixLen + 1 + 8

	// EntryVersion is the version of the stored entry value; versions 1 and 2 are refused.
	EntryVersion = 3
	// KeySize is the size of a stored entry key, the whole value under its KeyKey.
	KeySize = 32
	// MaxRecordSize is far above a real record, head or delete marker (about 200 bytes) and bounds decoding.
	MaxRecordSize = 1024
	// MaxProofSize bounds a stored consistency proof: two uvarints, a count and at most 65 hashes.
	MaxProofSize = 3*10 + 65*32
)

// LogPrefix returns 0x01 | id, the prefix of every key of the log.
func LogPrefix(id kololog.LogID) []byte {
	k := make([]byte, LogPrefixLen, EntryKeyLen)
	k[0] = PrefixLog
	copy(k[1:], id[:])
	return k
}

// LogPrefixEnd returns the first key after every key that starts with LogPrefix(id).
func LogPrefixEnd(id kololog.LogID) []byte {
	k := LogPrefix(id)
	for k[len(k)-1] == 0xff {
		k = k[:len(k)-1]
	}
	k[len(k)-1]++
	return k
}

// HeaderKey returns the key of the log header.
func HeaderKey(id kololog.LogID) []byte { return append(LogPrefix(id), SuffixHeader) }

// HeadKey returns the key of the latest signed head.
func HeadKey(id kololog.LogID) []byte { return append(LogPrefix(id), SuffixHead) }

// EntryKey returns the key of the entry at index, which sorts by index.
func EntryKey(id kololog.LogID, index uint64) []byte {
	return binary.BigEndian.AppendUint64(append(LogPrefix(id), SuffixEntry), index)
}

// KeyKey returns the key under which the key of the entry at index is stored, apart from its ciphertext, sorted by index.
func KeyKey(id kololog.LogID, index uint64) []byte {
	return binary.BigEndian.AppendUint64(append(LogPrefix(id), SuffixKey), index)
}

// ReplicaKind is the list value of a read-only replica; an own log has an empty list value.
var ReplicaKind = []byte{1}

// TargetKey returns the key of a replica's unfinished catch-up: the head it is filling and the proof from the stored entries to it.
func TargetKey(id kololog.LogID) []byte { return append(LogPrefix(id), SuffixTarget) }

// EncodeTarget stores an encoded head and an encoded consistency proof.
func EncodeTarget(head, proof []byte) []byte {
	var enc kolocodec.Encoder
	enc.PutBytes(head)
	enc.PutBytes(proof)
	return enc.Bytes()
}

// DecodeTarget strictly decodes a value made by [EncodeTarget].
func DecodeTarget(b []byte) (head, proof []byte, err error) {
	d := kolocodec.NewDecoder(b)
	if head, err = d.Bytes(MaxRecordSize); err != nil {
		return nil, nil, err
	}
	if proof, err = d.Bytes(MaxProofSize); err != nil {
		return nil, nil, err
	}
	if err := d.Done(); err != nil {
		return nil, nil, err
	}
	return head, proof, nil
}

// ListKey returns 0x02 | id, the key that lists the log.
func ListKey(id kololog.LogID) []byte {
	k := make([]byte, LogPrefixLen)
	k[0] = PrefixList
	copy(k[1:], id[:])
	return k
}

// EncodeEntry stores the record and either the ciphertext or, for a deleted entry, the delete marker; the key is stored under KeyKey.
func EncodeEntry(e kololog.Entry) []byte {
	deleted := len(e.Marker) > 0
	payload := e.Ciphertext
	if deleted {
		payload = e.Marker
	}
	var enc kolocodec.Encoder
	enc.PutUvarint(EntryVersion)
	enc.PutBytes(e.Record)
	enc.PutBool(deleted)
	enc.PutBytes(payload)
	return enc.Bytes()
}

// DecodeEntry strictly decodes a value made by [EncodeEntry].
func DecodeEntry(b []byte) (kololog.Entry, error) {
	d := kolocodec.NewDecoder(b)
	v, err := d.Uvarint()
	if err != nil {
		return kololog.Entry{}, err
	}
	if v != EntryVersion {
		return kololog.Entry{}, fmt.Errorf("unknown entry version %d", v)
	}
	var e kololog.Entry
	if e.Record, err = d.Bytes(MaxRecordSize); err != nil {
		return kololog.Entry{}, err
	}
	deleted, err := d.Bool()
	if err != nil {
		return kololog.Entry{}, err
	}
	max := d.Len()
	if deleted {
		max = MaxRecordSize
	}
	payload, err := d.Bytes(max)
	if err != nil {
		return kololog.Entry{}, err
	}
	if len(payload) == 0 {
		return kololog.Entry{}, errors.New("entry without ciphertext or delete marker")
	}
	if deleted {
		e.Marker = payload
	} else {
		e.Ciphertext = payload
	}
	if err := d.Done(); err != nil {
		return kololog.Entry{}, err
	}
	return e, nil
}

// DecodeKey checks a value stored under KeyKey and returns a copy of the key.
func DecodeKey(b []byte) (kololog.EntryKey, error) {
	if len(b) != KeySize {
		return nil, fmt.Errorf("entry key of %d bytes", len(b))
	}
	return kololog.EntryKey(bytes.Clone(b)), nil
}
