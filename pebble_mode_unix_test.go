//go:build unix

package kolostore

import (
	"crypto/rand"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// fileModes returns the permission bits of the regular files in dir.
func fileModes(t *testing.T, dir string) map[string]fs.FileMode {
	t.Helper()
	ents, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	modes := map[string]fs.FileMode{}
	for _, e := range ents {
		info, err := e.Info()
		if errors.Is(err, fs.ErrNotExist) { // Pebble removed it in the background
			continue
		}
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().IsRegular() {
			modes[e.Name()] = info.Mode().Perm()
		}
	}
	return modes
}

func TestPebbleFilesAreOwnerOnly(t *testing.T) {
	old := syscall.Umask(0o022)
	defer syscall.Umask(old)
	dir := pebbleDir(t)
	b, err := OpenPebbleWith(dir, PebbleOptions{MemTableSize: MinMemTableSize})
	if err != nil {
		t.Fatal(err)
	}
	for i := range 12 {
		v := make([]byte, 512<<10)
		rand.Read(v)
		if err := b.Put([]byte(fmt.Sprintf("k%02d", i)), v); err != nil {
			t.Fatal(err)
		}
	}
	// Checked after Close: a file is 0644 for an instant between its creation and the chmod.
	check := func(when string) {
		t.Helper()
		modes := fileModes(t, dir)
		if len(modes) < 4 {
			t.Fatalf("%s: only %d files in the directory", when, len(modes))
		}
		sst := false
		for name, mode := range modes {
			sst = sst || strings.HasSuffix(name, ".sst")
			if mode != 0o600 {
				t.Errorf("%s: %s has mode %o, want 600", when, name, mode)
			}
		}
		if !sst {
			t.Errorf("%s: no SST file; the memtable never flushed", when)
		}
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	check("closed")
}

func TestPebbleReopenRestrictsOldFiles(t *testing.T) {
	dir := pebbleDir(t)
	b, err := OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := b.Put([]byte("k"), []byte("v")); err != nil {
		t.Fatal(err)
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	for name := range fileModes(t, dir) {
		if err := os.Chmod(filepath.Join(dir, name), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	b, err = OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	for name, mode := range fileModes(t, dir) {
		if mode != 0o600 {
			t.Errorf("%s has mode %o after reopen, want 600", name, mode)
		}
	}
	if got, err := b.Get([]byte("k")); err != nil || string(got) != "v" {
		t.Fatalf("Get after reopen: %q %v", got, err)
	}
}
