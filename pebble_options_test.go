package kolostore

import (
	"bytes"
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"testing"
	"time"
)

func TestPebbleOptionsDefaults(t *testing.T) {
	o := pebbleOptions(PebbleOptions{})
	if o.MemTableSize != 0 {
		t.Fatalf("MemTableSize %d, want 0 so Pebble keeps its default", o.MemTableSize)
	}
	for i, l := range o.Levels {
		if l.Compression != nil {
			t.Fatalf("level %d sets a compression", i)
		}
	}
	if o.FormatMajorVersion != pebbleFormat {
		t.Fatalf("format %d", o.FormatMajorVersion)
	}
	o.EnsureDefaults()
	if o.MemTableSize != 4<<20 {
		t.Fatalf("default MemTableSize %d", o.MemTableSize)
	}
}

func TestPebbleOptionsTuned(t *testing.T) {
	o := pebbleOptions(PebbleOptions{MemTableSize: 64 << 20, NoCompression: true})
	if o.MemTableSize != 64<<20 {
		t.Fatalf("MemTableSize %d", o.MemTableSize)
	}
	o.EnsureDefaults()
	for i, l := range o.Levels {
		if p := l.Compression(); p.Name != "NoCompression" {
			t.Fatalf("level %d compression %q", i, p.Name)
		}
	}
}

func TestOpenPebbleWithInvalid(t *testing.T) {
	for _, size := range []int64{-1, 1, MinMemTableSize - 1, MaxMemTableSize + 1} {
		dir := pebbleDir(t)
		b, err := OpenPebbleWith(dir, PebbleOptions{MemTableSize: size})
		if b != nil || !errors.Is(err, ErrInvalidOptions) {
			t.Errorf("MemTableSize %d: %v, %v", size, b, err)
		}
		if _, err := os.Stat(dir); !errors.Is(err, fs.ErrNotExist) {
			t.Errorf("MemTableSize %d created the directory: %v", size, err)
		}
	}
	for _, size := range []int64{0, MinMemTableSize, MaxMemTableSize} {
		b, err := OpenPebbleWith(pebbleDir(t), PebbleOptions{MemTableSize: size})
		if err != nil {
			t.Fatalf("MemTableSize %d: %v", size, err)
		}
		b.Close()
	}
}

// copyDir returns an atomic image of the open database in a new 0700 directory, as a crash would leave it.
// Copying the files one by one is not atomic while flushes rewrite them.
func copyDir(t *testing.T, b Backend) string {
	t.Helper()
	dst := pebbleDir(t)
	if err := b.(*pebbleBackend).db.Checkpoint(dst); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dst, 0o700); err != nil { // Checkpoint creates it 0755, which OpenPebble refuses
		t.Fatal(err)
	}
	return dst
}

func TestOpenPebbleWithRoundtrip(t *testing.T) {
	tuned := PebbleOptions{MemTableSize: 4 << 20, NoCompression: true}
	dir := pebbleDir(t)
	b, err := OpenPebbleWith(dir, tuned)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	want := map[string][]byte{}
	for i := range 12 {
		v := make([]byte, 1<<20)
		rand.Read(v)
		k := fmt.Sprintf("k%02d", i)
		want[k] = v
		if err := b.Put([]byte(k), v); err != nil {
			t.Fatal(err)
		}
	}
	check := func(name string, b Backend) {
		t.Helper()
		for k, v := range want {
			got, err := b.Get([]byte(k))
			if err != nil || !bytes.Equal(got, v) {
				t.Fatalf("%s: %s: %v", name, k, err)
			}
		}
	}
	check("open", b)
	// Every Put was synced, so an image taken now, without Close, holds all of them.
	crashed := copyDir(t, b)
	for _, o := range []PebbleOptions{tuned, {}} {
		c, err := OpenPebbleWith(crashed, o)
		if err != nil {
			t.Fatal(err)
		}
		check("after crash", c)
		if err := c.Close(); err != nil {
			t.Fatal(err)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	// Reopen with the defaults and the tuned options.
	for _, o := range []PebbleOptions{{}, tuned} {
		b, err := OpenPebbleWith(dir, o)
		if err != nil {
			t.Fatal(err)
		}
		check("reopen", b)
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestPebbleWriteSpeed writes 1 GiB of 1 MiB random values with the default and the tuned options and prints MB/s.
// It needs KOLO_STORE_BENCH=1; KOLO_STORE_BENCH_BYTES sets another size.
func TestPebbleWriteSpeed(t *testing.T) {
	if os.Getenv("KOLO_STORE_BENCH") != "1" {
		t.Skip("set KOLO_STORE_BENCH=1 to run")
	}
	total := int64(1 << 30)
	if s := os.Getenv("KOLO_STORE_BENCH_BYTES"); s != "" {
		if _, err := fmt.Sscan(s, &total); err != nil || total < 1<<20 {
			t.Fatalf("KOLO_STORE_BENCH_BYTES %q", s)
		}
	}
	const size = 1 << 20
	val := make([]byte, size)
	for _, c := range []struct {
		name string
		o    PebbleOptions
	}{
		{"default", PebbleOptions{}},
		{"tuned (256 MiB memtable, no compression)", PebbleOptions{MemTableSize: 256 << 20, NoCompression: true}},
	} {
		b, err := OpenPebbleWith(pebbleDir(t), c.o)
		if err != nil {
			t.Fatal(err)
		}
		start := time.Now()
		for i := range total / size {
			rand.Read(val)
			if err := b.Put(fmt.Appendf(nil, "chunk%08d", i), val); err != nil {
				t.Fatal(err)
			}
		}
		secs := time.Since(start).Seconds()
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("%s: %d MiB in %.1f s, %.0f MB/s", c.name, total>>20, secs, float64(total)/1e6/secs)
	}
}
