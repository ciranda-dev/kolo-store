package kolostore

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	kololog "github.com/ciranda-dev/kolo-log"
	"github.com/ciranda-dev/kolo-store/internal/layout"
)

const (
	largeEntry = 1 << 20 // bytes per entry, about one kolo-blobs chunk
	largeChunk = 4       // entries per replica Update
	// largeDefault keeps the test short in a normal run; KOLO_STORE_LARGE_BYTES sets another size, such as 10 GiB.
	largeDefault = 256 << 20
	// Bounds checked by TestLargeLogBoundedMemory: a fixed allowance for the GC's headroom under the 64 MiB limit, a few entries
	// in flight and Pebble's flush and compaction buffers, plus a few hundred bytes per entry.
	largeHeapBase  = 112 << 20
	largeHeapEntry = 1 << 10
	largeKeptEntry = 512
	// Adopt holds the entries it adds twice, in its batch and in Pebble's, measured at 2.1 times their size.
	largeAdoptQuarters = 9
)

// heapPeak samples HeapInuse every few milliseconds until stop is called and returns the largest sample.
func heapPeak() (stop func() uint64) {
	var peak uint64
	done := make(chan struct{})
	var wg sync.WaitGroup
	wg.Go(func() {
		var ms runtime.MemStats
		for {
			runtime.ReadMemStats(&ms)
			peak = max(peak, ms.HeapInuse)
			select {
			case <-done:
				return
			case <-time.After(5 * time.Millisecond):
			}
		}
	})
	return func() uint64 {
		close(done)
		wg.Wait()
		return peak
	}
}

func largeValue(v []byte, i uint64) []byte {
	binary.BigEndian.PutUint64(v, i)
	return v
}

func heapAlloc() uint64 {
	runtime.GC()
	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)
	return ms.HeapAlloc
}

// TestLargeLogChild writes a log of KOLO_STORE_LARGE 1 MiB entries to Pebble, replicates it in chunks, reopens, reads
// everything back and adopts the replica into a third store, printing the peak heap of each step.
func TestLargeLogChild(t *testing.T) {
	n, err := strconv.ParseUint(os.Getenv("KOLO_STORE_LARGE"), 10, 64)
	if err != nil || n == 0 {
		t.Skip("run by TestLargeLogBoundedMemory")
	}
	dir := t.TempDir()
	var read atomic.Int64 // value bytes the backends returned
	openPebble := func(name string) *Store {
		b, err := OpenPebble(filepath.Join(dir, name))
		if err != nil {
			t.Fatal(err)
		}
		return New(&countingReads{Backend: b, n: &read})
	}
	id := testIdentity(t)
	var report []string
	step := func(name string, fn func()) {
		start := time.Now()
		stop := heapPeak()
		fn()
		report = append(report, fmt.Sprintf("%s=%d/%s", name, stop(), time.Since(start).Round(time.Millisecond)))
	}

	s, s2, dev := openPebble("owner"), openPebble("replica"), openPebble("device")
	defer dev.Close()
	var logID kololog.LogID
	var header []byte
	step("write", func() {
		l := mustCreate(t, s, id)
		logID, header = l.ID(), l.Header().Encode()
		// The device holds the empty log, so it adopts every entry.
		for k, v := range snapshot(t, s.b) {
			putRaw(t, dev.b, []byte(k), []byte(v))
		}
		v := make([]byte, largeEntry)
		for i := range n {
			mustAppend(t, l, largeValue(v, i))
		}
		l.Close()
	})
	step("replicate", func() {
		l, err := s.Open(id, logID)
		if err != nil {
			t.Fatal(err)
		}
		defer l.Close()
		r := mustOpenReplica(t, s2, id.PublicKey(), logID, header)
		defer r.Close()
		h := mustHead(t, l)
		for from := uint64(0); from < n; from += largeChunk {
			to := min(from+largeChunk, n)
			p, err := l.ConsistencyProof(to, n)
			if err != nil {
				t.Fatal(err)
			}
			var entries []kololog.Entry
			for i := from; i < to; i++ {
				e, err := l.Entry(i)
				if err != nil {
					t.Fatal(err)
				}
				entries = append(entries, e)
			}
			if err := r.Update(h, p, entries); err != nil {
				t.Fatal(err)
			}
			for _, e := range entries {
				clear(e.Key)
			}
		}
	})
	s.Close()
	s2.Close()

	s, s2 = openPebble("owner"), openPebble("replica")
	defer s.Close()
	defer s2.Close()
	var l *Log
	var r *Replica
	step("open", func() {
		if l, err = s.Open(id, logID); err != nil {
			t.Fatal(err)
		}
		r = mustOpenReplica(t, s2, id.PublicKey(), logID, nil)
	})
	// What a sync layer's idle pass needs, whether each entry is deleted and its key, must read no ciphertext.
	read.Store(0)
	step("info", func() {
		for i := range n {
			for _, rd := range []reader{l, r} {
				info, err := rd.(interface {
					EntryInfo(uint64) (kololog.EntryInfo, error)
				}).EntryInfo(i)
				if err != nil || info != (kololog.EntryInfo{Keyed: true}) {
					t.Fatalf("EntryInfo(%d) = %+v, %v", i, info, err)
				}
				k, err := rd.(interface {
					Key(uint64) (kololog.EntryKey, error)
				}).Key(i)
				if err != nil {
					t.Fatal(err)
				}
				clear(k)
			}
		}
	})
	infoRead := read.Load()
	step("read", func() {
		for i := range n {
			for _, rd := range []reader{l, r} {
				v, err := rd.Get(i)
				if err != nil || len(v) != largeEntry || binary.BigEndian.Uint64(v) != i {
					t.Fatalf("entry %d reads back %d bytes, %v", i, len(v), err)
				}
			}
		}
	})
	if l.Len() != n || r.Len() != n {
		t.Fatalf("Len %d and %d, want %d", l.Len(), r.Len(), n)
	}
	// What the open log and replica keep is the heap they hold minus the heap once they are closed, on the same open stores.
	open := heapAlloc()
	l.Close()
	r.Close()
	l, r = nil, nil
	closed := heapAlloc()
	kept := open - min(open, closed)
	r = mustOpenReplica(t, s2, id.PublicKey(), logID, nil)
	step("adopt", func() {
		l, err := dev.Adopt(id, r)
		if n*largeEntry >= adoptLimit {
			if !errors.Is(err, ErrTooLarge) {
				t.Fatalf("Adopt of %d entries: %v, want ErrTooLarge", n, err)
			}
			return
		}
		if err != nil || l.Len() != n {
			t.Fatalf("Adopt of %d entries: %v", n, err)
		}
		l.Close()
	})
	r.Close()
	fmt.Printf("LARGE entries=%d kept=%d inforead=%d %s\n", n, kept, infoRead, strings.Join(report, " "))
}

// A log of 1 MiB entries goes through the store on Pebble in a child process under GOMEMLIMIT=64MiB. The peak heap of
// every step but Adopt stays within a fixed allowance plus a few hundred bytes per entry, Adopt adds at most 2.25 times
// the entries it adds, an open log or replica keeps under 512 bytes per entry, and EntryInfo and Key read no ciphertext.
func TestLargeLogBoundedMemory(t *testing.T) {
	if testing.Short() || raceEnabled {
		t.Skip("writes and verifies hundreds of MiB; skipped with -short and -race")
	}
	size := uint64(largeDefault)
	if v := os.Getenv("KOLO_STORE_LARGE_BYTES"); v != "" {
		var err error
		if size, err = strconv.ParseUint(v, 10, 64); err != nil || size < largeEntry {
			t.Fatalf("KOLO_STORE_LARGE_BYTES=%q: want a number of bytes of at least %d", v, largeEntry)
		}
	}
	n := size / largeEntry
	cmd := exec.Command(os.Args[0], "-test.run=^TestLargeLogChild$", "-test.count=1", "-test.timeout=0")
	cmd.Env = append(os.Environ(), "KOLO_STORE_LARGE="+strconv.FormatUint(n, 10), "GOMEMLIMIT=64MiB")
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	var line []string
	sc := bufio.NewScanner(out)
	for sc.Scan() {
		if f := strings.Fields(sc.Text()); len(f) > 0 && f[0] == "LARGE" {
			line = f[1:]
		}
	}
	if err := cmd.Wait(); err != nil || line == nil {
		t.Fatalf("child: %v, report %q", err, line)
	}
	t.Logf("%d MiB, peak resident size %d MiB: %s", size>>20, maxRSS(cmd.ProcessState)>>20, strings.Join(line, " "))
	var kept, infoRead uint64
	fmt.Sscanf(line[1], "kept=%d", &kept)
	fmt.Sscanf(line[2], "inforead=%d", &infoRead)
	if kept > largeKeptEntry*2*n {
		t.Errorf("open log and replica keep %d bytes of heap, %d per entry, want at most %d", kept, kept/(2*n), largeKeptEntry)
	}
	if want := uint64(2 * n * layout.KeySize); infoRead != want {
		t.Errorf("EntryInfo and Key of every entry read %d bytes, want only the %d bytes of the keys", infoRead, want)
	}
	for _, f := range line[3:] {
		name, rest, _ := strings.Cut(f, "=")
		heap, _ := strconv.ParseUint(strings.Split(rest, "/")[0], 10, 64)
		limit := largeHeapBase + largeHeapEntry*2*n
		if name == "adopt" {
			limit = largeHeapBase + largeAdoptQuarters*min(size, adoptLimit)/4
		}
		if heap > limit {
			t.Errorf("%s: peak heap %d bytes, want at most %d", name, heap, limit)
		}
	}
}

// countingReads adds up the value bytes its backend returns.
type countingReads struct {
	Backend
	n *atomic.Int64
}

func (c *countingReads) Get(key []byte) ([]byte, error) {
	v, err := c.Backend.Get(key)
	c.n.Add(int64(len(v)))
	return v, err
}

func (c *countingReads) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	return c.Backend.Scan(start, end, reverse, limit, func(k, v []byte) bool {
		c.n.Add(int64(len(v)))
		return fn(k, v)
	})
}
