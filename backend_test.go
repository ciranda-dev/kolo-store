package kolostore

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"testing"
)

// storage is one underlying store that can be opened again after its backend is closed.
type storage interface {
	open(t testing.TB) Backend
}

type memStorage struct{ m Backend }

func (s memStorage) open(testing.TB) Backend { return keepOpen{s.m} }

// keepOpen lets a test close a memory backend and open the same data again.
type keepOpen struct{ Backend }

func (keepOpen) Close() error { return nil }

type pebbleStorage struct{ dir string }

// pebbleDir returns a path in t.TempDir that OpenPebble creates with mode 0700.
func pebbleDir(t testing.TB) string {
	return filepath.Join(t.TempDir(), "db")
}

func (s pebbleStorage) open(t testing.TB) Backend {
	t.Helper()
	b, err := OpenPebble(s.dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

var kinds = []struct {
	name string
	new  func(t testing.TB) storage
}{
	{"memory", func(testing.TB) storage { return memStorage{NewMemory()} }},
	{"pebble", func(t testing.TB) storage { return pebbleStorage{pebbleDir(t)} }},
}

func forEachBackend(t *testing.T, fn func(t *testing.T, st storage)) {
	for _, k := range kinds {
		t.Run(k.name, func(t *testing.T) { fn(t, k.new(t)) })
	}
}

func put(t testing.TB, b Backend, kvs ...string) {
	t.Helper()
	for i := 0; i < len(kvs); i += 2 {
		if err := b.Put([]byte(kvs[i]), []byte(kvs[i+1])); err != nil {
			t.Fatal(err)
		}
	}
}

func scanKeys(t testing.TB, b Backend, start, end string, reverse bool, limit int) []string {
	t.Helper()
	var s, e []byte
	if start != "" {
		s = []byte(start)
	}
	if end != "" {
		e = []byte(end)
	}
	var keys []string
	err := b.Scan(s, e, reverse, limit, func(k, v []byte) bool {
		if string(v) != "v"+string(k) {
			t.Errorf("value of %q is %q", k, v)
		}
		keys = append(keys, string(k))
		return true
	})
	if err != nil {
		t.Fatal(err)
	}
	return keys
}

func TestBackendGetPutDelete(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		if _, err := b.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get missing: %v, want ErrNotFound", err)
		}
		put(t, b, "a", "1", "a", "2")
		if v, err := b.Get([]byte("a")); err != nil || string(v) != "2" {
			t.Fatalf("Get = %q, %v, want 2", v, err)
		}
		if err := b.Put([]byte("empty"), nil); err != nil {
			t.Fatal(err)
		}
		if v, err := b.Get([]byte("empty")); err != nil || v == nil || len(v) != 0 {
			t.Fatalf("Get empty = %#v, %v, want empty non-nil", v, err)
		}
		if err := b.Delete([]byte("a")); err != nil {
			t.Fatal(err)
		}
		if err := b.Delete([]byte("missing")); err != nil {
			t.Fatalf("Delete missing: %v", err)
		}
		if _, err := b.Get([]byte("a")); !errors.Is(err, ErrNotFound) {
			t.Fatalf("Get deleted: %v, want ErrNotFound", err)
		}
	})
}

func TestBackendScan(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		put(t, b, "e", "ve", "a", "va", "c", "vc", "b", "vb", "d", "vd")
		tests := []struct {
			start, end string
			reverse    bool
			limit      int
			want       []string
		}{
			{"", "", false, 0, []string{"a", "b", "c", "d", "e"}},
			{"", "", true, 0, []string{"e", "d", "c", "b", "a"}},
			{"b", "d", false, 0, []string{"b", "c"}},
			{"b", "d", true, 0, []string{"c", "b"}},
			{"bb", "dd", false, 0, []string{"c", "d"}},
			{"a", "", false, 2, []string{"a", "b"}},
			{"a", "", true, 2, []string{"e", "d"}},
			{"b", "e", true, 1, []string{"d"}},
			{"a", "z", false, -1, []string{"a", "b", "c", "d", "e"}},
			{"d", "b", false, 0, nil},
			{"c", "c", false, 0, nil},
			{"f", "", false, 0, nil},
		}
		for _, tt := range tests {
			got := scanKeys(t, b, tt.start, tt.end, tt.reverse, tt.limit)
			if !slices.Equal(got, tt.want) {
				t.Errorf("Scan(%q, %q, reverse=%v, limit=%d) = %q, want %q", tt.start, tt.end, tt.reverse, tt.limit, got, tt.want)
			}
		}
		n := 0
		if err := b.Scan(nil, nil, false, 0, func(k, v []byte) bool { n++; return n < 2 }); err != nil {
			t.Fatal(err)
		}
		if n != 2 {
			t.Errorf("Scan called fn %d times after it returned false, want 2", n)
		}
	})
}

func TestBackendBatch(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		put(t, b, "a", "va", "b", "vb")
		batch := b.NewBatch()
		batch.Put([]byte("c"), []byte("vc"))
		batch.Delete([]byte("a"))
		batch.Put([]byte("b"), []byte("vb"))
		if got := scanKeys(t, b, "", "", false, 0); !slices.Equal(got, []string{"a", "b"}) {
			t.Fatalf("before Commit: %q", got)
		}
		if err := batch.Commit(); err != nil {
			t.Fatal(err)
		}
		if got := scanKeys(t, b, "", "", false, 0); !slices.Equal(got, []string{"b", "c"}) {
			t.Fatalf("after Commit: %q, want [b c]", got)
		}
		if err := batch.Commit(); err == nil {
			t.Error("second Commit succeeded")
		}

		abandoned := b.NewBatch()
		abandoned.Put([]byte("x"), []byte("vx"))
		if _, err := b.Get([]byte("x")); !errors.Is(err, ErrNotFound) {
			t.Errorf("uncommitted write is visible: %v", err)
		}
	})
}

func TestBackendDeleteRange(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		put(t, b, "a", "va", "b", "vb", "bz", "vbz", "c", "vc", "d", "vd")
		batch := b.NewBatch()
		batch.DeleteRange([]byte("b"), []byte("d"))
		batch.DeleteRange([]byte("d"), []byte("a")) // empty range
		batch.Put([]byte("c"), []byte("vc"))        // a later write in the batch wins
		if err := batch.Commit(); err != nil {
			t.Fatal(err)
		}
		if got := scanKeys(t, b, "", "", false, 0); !slices.Equal(got, []string{"a", "c", "d"}) {
			t.Fatalf("after DeleteRange: %q, want [a c d]", got)
		}
	})
}

func TestBackendCopies(t *testing.T) {
	forEachBackend(t, func(t *testing.T, st storage) {
		b := st.open(t)
		k, v := []byte("k"), []byte("vk")
		if err := b.Put(k, v); err != nil {
			t.Fatal(err)
		}
		k[0], v[0] = 'x', 'x'
		got, err := b.Get([]byte("k"))
		if err != nil || string(got) != "vk" {
			t.Fatalf("Get after caller changed Put args = %q, %v", got, err)
		}
		got[0] = 'x'
		if again, _ := b.Get([]byte("k")); string(again) != "vk" {
			t.Fatalf("changing a Get result changed the store: %q", again)
		}
		b.Scan(nil, nil, false, 0, func(k, v []byte) bool {
			k[0], v[0] = 'x', 'x'
			return true
		})
		if got := scanKeys(t, b, "", "", false, 0); !slices.Equal(got, []string{"k"}) {
			t.Fatalf("changing Scan arguments changed the store: %q", got)
		}

		batch := b.NewBatch()
		bk, bv := []byte("m"), []byte("vm")
		batch.Put(bk, bv)
		bk[0], bv[0] = 'x', 'x'
		if err := batch.Commit(); err != nil {
			t.Fatal(err)
		}
		if got, err := b.Get([]byte("m")); err != nil || !bytes.Equal(got, []byte("vm")) {
			t.Fatalf("batch kept the caller's slices: %q, %v", got, err)
		}
	})
}

func TestBackendClose(t *testing.T) {
	for _, b := range []Backend{NewMemory(), pebbleStorage{pebbleDir(t)}.open(t)} {
		batch := b.NewBatch()
		batch.Put([]byte("a"), nil)
		if err := b.Close(); err != nil {
			t.Fatal(err)
		}
		if _, err := b.Get([]byte("a")); !errors.Is(err, ErrClosed) {
			t.Errorf("%T: Get after Close: %v", b, err)
		}
		if err := b.Put([]byte("a"), nil); !errors.Is(err, ErrClosed) {
			t.Errorf("%T: Put after Close: %v", b, err)
		}
		if err := b.Scan(nil, nil, false, 0, func(k, v []byte) bool { return true }); !errors.Is(err, ErrClosed) {
			t.Errorf("%T: Scan after Close: %v", b, err)
		}
		if err := batch.Commit(); !errors.Is(err, ErrClosed) {
			t.Errorf("%T: Commit after Close: %v", b, err)
		}
		if err := b.Close(); err != nil {
			t.Errorf("%T: second Close: %v", b, err)
		}
	}
}

func TestPebbleReopen(t *testing.T) {
	st := pebbleStorage{pebbleDir(t)}
	b := st.open(t)
	put(t, b, "a", "va")
	if second, err := OpenPebble(st.dir); err == nil {
		second.Close()
		t.Fatal("second OpenPebble on a locked dir succeeded")
	}
	if err := b.Close(); err != nil {
		t.Fatal(err)
	}
	b = st.open(t)
	if got := scanKeys(t, b, "", "", false, 0); !slices.Equal(got, []string{"a"}) {
		t.Fatalf("after reopen: %q", got)
	}
}

func TestPebbleSameDirOtherSpelling(t *testing.T) {
	dir := pebbleDir(t)
	b, err := OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(dir, link); err != nil {
		t.Fatal(err)
	}
	b, err = OpenPebble(dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Chdir(filepath.Dir(dir))
	for _, other := range []string{link, dir + "/.", dir + "//", filepath.Base(dir)} {
		if second, err := OpenPebble(other); err == nil {
			second.Close()
			t.Errorf("OpenPebble(%q) of an open directory succeeded", other)
		}
	}
	b.Close()
	b, err = OpenPebble(link)
	if err != nil {
		t.Fatalf("OpenPebble through the link after Close: %v", err)
	}
	b.Close()
}

func TestPebbleInsecureDir(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no unix permissions")
	}
	open := filepath.Join(t.TempDir(), "open")
	if err := os.Mkdir(open, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(open, 0o777); err != nil {
		t.Fatal(err)
	}
	if b, err := OpenPebble(open); !errors.Is(err, ErrInsecureDir) {
		if err == nil {
			b.Close()
		}
		t.Fatalf("OpenPebble of a 0777 directory: %v, want ErrInsecureDir", err)
	}
	private := filepath.Join(t.TempDir(), "private")
	if err := os.Mkdir(private, 0o700); err != nil {
		t.Fatal(err)
	}
	b, err := OpenPebble(private)
	if err != nil {
		t.Fatalf("OpenPebble of a 0700 directory: %v", err)
	}
	b.Close()
	created := filepath.Join(t.TempDir(), "a", "b")
	b, err = OpenPebble(created)
	if err != nil {
		t.Fatal(err)
	}
	b.Close()
	if fi, err := os.Stat(created); err != nil || fi.Mode().Perm() != 0o700 {
		t.Fatalf("created directory mode %v, %v, want 0700", fi.Mode().Perm(), err)
	}
}

func TestPebbleDirSwappedBeforeOpen(t *testing.T) {
	if runtime.GOOS == "windows" {
		t.Skip("no unix permissions")
	}
	good, bad := pebbleDir(t), filepath.Join(t.TempDir(), "bad")
	for _, d := range []string{good, bad} {
		if err := os.Mkdir(d, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Chmod(bad, 0o777); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(t.TempDir(), "link")
	if err := os.Symlink(good, link); err != nil {
		t.Fatal(err)
	}
	beforePebbleOpen.set(func(string) {
		os.Remove(link)
		if err := os.Symlink(bad, link); err != nil {
			t.Error(err)
		}
	})
	defer beforePebbleOpen.set(nil)
	b, err := OpenPebble(link)
	if err == nil {
		b.Close()
		t.Fatal("OpenPebble succeeded after the directory was swapped for a world-writable one")
	}
	if !errors.Is(err, ErrInsecureDir) {
		t.Fatalf("OpenPebble after the swap: %v, want ErrInsecureDir", err)
	}
	beforePebbleOpen.set(nil)
	if b, err := OpenPebble(bad); !errors.Is(err, ErrInsecureDir) {
		if err == nil {
			b.Close()
		}
		t.Fatalf("the swapped directory is left open: %v", err)
	}
	if b, err := OpenPebble(good); err != nil {
		t.Fatalf("OpenPebble of the original directory: %v", err)
	} else {
		b.Close()
	}
}
