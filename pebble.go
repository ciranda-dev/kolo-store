package kolostore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io/fs"
	"math"
	"os"
	"path/filepath"
	"slices"
	"sync"

	"github.com/ciranda-dev/kolo-store/internal/securedir"
	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// pebbleFormat is pinned so a Pebble upgrade never rewrites the on-disk format by itself.
const pebbleFormat = pebble.FormatTableFormatV6

// pebbleBatchLimit is Pebble's batch size limit, min(MaxUint32, MaxInt) bytes: a batch that reaches it panics in Batch.grow.
var pebbleBatchLimit uint64 = min(math.MaxUint32, math.MaxInt)

// batchCost is what Pebble reserves for one write in a batch: a kind byte, two 32-bit varint lengths, the key and the value.
func batchCost(key, value []byte) uint64 {
	return 1 + 2*binary.MaxVarintLen32 + uint64(len(key)) + uint64(len(value))
}

// pebbleBatchHeader is the fixed header of a Pebble batch, batchrepr.HeaderLen.
const pebbleBatchHeader = 12

type pebbleBackend struct {
	mu     sync.RWMutex // Pebble panics on use after Close
	db     *pebble.DB
	dir    os.FileInfo
	closed bool
}

// ErrInsecureDir is returned by [OpenPebble] for a directory that group or others can access, that another user owns or that changed while opening.
var ErrInsecureDir = errors.New("kolostore: insecure directory")

var errDirOpen = errors.New("kolostore: directory already open in this process")

// openDirs compares directories with os.SameFile, since Pebble's own check misses another spelling of the same path.
var openDirs struct {
	sync.Mutex
	list []os.FileInfo
}

// beforePebbleOpen lets tests change the directory between the checks and pebble.Open.
var beforePebbleOpen hook[func(dir string)]

// quietLogger drops Pebble's informational messages; errors still go to the standard logger.
type quietLogger struct{ pebble.Logger }

func (quietLogger) Infof(string, ...any) {}

const (
	// MinMemTableSize is the smallest [PebbleOptions.MemTableSize], 1 MiB.
	MinMemTableSize = 1 << 20
	// MaxMemTableSize is the largest [PebbleOptions.MemTableSize], 1 GiB.
	MaxMemTableSize = 1 << 30
)

// ErrInvalidOptions is returned by [OpenPebbleWith] for a [PebbleOptions] value out of bounds.
var ErrInvalidOptions = errors.New("kolostore: invalid pebble options")

// PebbleOptions tunes [OpenPebbleWith]; the zero value is what [OpenPebble] uses. Neither field changes durability: every commit is still synced.
type PebbleOptions struct {
	// MemTableSize is the size in bytes of Pebble's memtable, between [MinMemTableSize] and [MaxMemTableSize]; zero keeps Pebble's default of 4 MiB.
	// A larger memtable flushes less often, which speeds up writes of large values, and costs up to twice its size in memory.
	MemTableSize int64
	// NoCompression stores blocks uncompressed. Use it when the values are ciphertext, which does not compress.
	// A directory written with it opens with or without it.
	NoCompression bool
}

// OpenPebble opens or creates a Pebble [Backend] in dir; another open of the same dir fails until Close.
// It creates dir with mode 0700 and returns [ErrInsecureDir] for a dir open to group or others, owned by another user or swapped while opening.
// Permissions and owner are not checked on Windows. The directory is checked only when it is opened;
// changing what dir points to afterwards needs write access to a parent directory, and the store does not defend against that.
func OpenPebble(dir string) (Backend, error) {
	return OpenPebbleWith(dir, PebbleOptions{})
}

// OpenPebbleWith is [OpenPebble] with tuning for large values; it returns [ErrInvalidOptions] before touching dir for a MemTableSize out of bounds.
// Every commit is still synced to disk, whatever the options.
func OpenPebbleWith(dir string, o PebbleOptions) (Backend, error) {
	if o.MemTableSize != 0 && (o.MemTableSize < MinMemTableSize || o.MemTableSize > MaxMemTableSize) {
		return nil, fmt.Errorf("%w: MemTableSize %d is not 0 or between %d and %d", ErrInvalidOptions, o.MemTableSize, MinMemTableSize, MaxMemTableSize)
	}
	fi, err := securedir.Prepare(dir, ErrInsecureDir)
	if err != nil {
		return nil, err
	}
	openDirs.Lock()
	defer openDirs.Unlock()
	for _, open := range openDirs.list {
		if os.SameFile(open, fi) {
			return nil, fmt.Errorf("%w: %s", errDirOpen, dir)
		}
	}
	if f := beforePebbleOpen.get(); f != nil {
		f(dir)
	}
	db, err := pebble.Open(dir, pebbleOptions(o))
	if err != nil {
		return nil, fmt.Errorf("kolostore: open pebble: %w", err)
	}
	// The path may have been swapped while Pebble opened it.
	if err := securedir.Recheck(dir, fi, ErrInsecureDir); err != nil {
		db.Close()
		return nil, err
	}
	if err := restrictFiles(dir); err != nil {
		db.Close()
		return nil, err
	}
	openDirs.list = append(openDirs.list, fi)
	return &pebbleBackend{db: db, dir: fi}, nil
}

// privateFS makes every file Pebble creates readable by its owner only; Pebble would use the process umask, usually 0644.
// The files hold the entry keys in clear, so they do not rely on the directory mode alone.
type privateFS struct{ vfs.FS }

func (f privateFS) Create(name string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.Create(name, c)
	return restrict(name, file, err)
}

func (f privateFS) ReuseForWrite(oldname, newname string, c vfs.DiskWriteCategory) (vfs.File, error) {
	file, err := f.FS.ReuseForWrite(oldname, newname, c)
	return restrict(newname, file, err)
}

func (f privateFS) OpenReadWrite(name string, c vfs.DiskWriteCategory, opts ...vfs.OpenOption) (vfs.File, error) {
	file, err := f.FS.OpenReadWrite(name, c, opts...)
	return restrict(name, file, err)
}

func (f privateFS) Unwrap() vfs.FS { return f.FS }

func restrict(name string, file vfs.File, err error) (vfs.File, error) {
	if err != nil {
		return nil, err
	}
	if err := os.Chmod(name, 0o600); err != nil {
		file.Close()
		return nil, fmt.Errorf("kolostore: %w", err)
	}
	return file, nil
}

// restrictFiles makes the files of a database written by an earlier version 0600 as well.
func restrictFiles(dir string) error {
	ents, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("kolostore: %w", err)
	}
	for _, e := range ents {
		if e.Type().IsRegular() {
			// Pebble removes obsolete files in the background right after it opens.
			if err := os.Chmod(filepath.Join(dir, e.Name()), 0o600); err != nil && !errors.Is(err, fs.ErrNotExist) {
				return fmt.Errorf("kolostore: %w", err)
			}
		}
	}
	return nil
}

// pebbleOptions leaves every field of a zero o unset, so Pebble applies its defaults.
func pebbleOptions(o PebbleOptions) *pebble.Options {
	opts := &pebble.Options{
		FormatMajorVersion: pebbleFormat,
		Logger:             quietLogger{pebble.DefaultLogger},
		FS:                 privateFS{vfs.Default},
		MemTableSize:       uint64(o.MemTableSize),
	}
	if o.NoCompression {
		opts.ApplyCompressionSettings(func() pebble.DBCompressionSettings { return pebble.DBCompressionNone })
	}
	return opts
}

func releaseDir(fi os.FileInfo) {
	openDirs.Lock()
	defer openDirs.Unlock()
	openDirs.list = slices.DeleteFunc(openDirs.list, func(o os.FileInfo) bool { return o == fi })
}

func (p *pebbleBackend) Get(key []byte) ([]byte, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return nil, ErrClosed
	}
	v, closer, err := p.db.Get(key)
	if errors.Is(err, pebble.ErrNotFound) {
		return nil, ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("kolostore: %w", err)
	}
	defer closer.Close()
	return clone(v), nil
}

func (p *pebbleBackend) Put(key, value []byte) error {
	b := p.NewBatch()
	b.Put(key, value)
	return b.Commit()
}

func (p *pebbleBackend) Delete(key []byte) error {
	b := p.NewBatch()
	b.Delete(key)
	return b.Commit()
}

func (p *pebbleBackend) Scan(start, end []byte, reverse bool, limit int, fn func(key, value []byte) bool) error {
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	if end != nil && bytes.Compare(start, end) >= 0 {
		return nil
	}
	it, err := p.db.NewIter(&pebble.IterOptions{LowerBound: start, UpperBound: end})
	if err != nil {
		return fmt.Errorf("kolostore: %w", err)
	}
	ok, next := it.First(), it.Next
	if reverse {
		ok, next = it.Last(), it.Prev
	}
	for n := 0; ok && (limit <= 0 || n < limit); n++ {
		v, err := it.ValueAndErr()
		if err != nil {
			it.Close()
			return fmt.Errorf("kolostore: %w", err)
		}
		if !fn(clone(it.Key()), clone(v)) {
			break
		}
		ok = next()
	}
	if err := it.Close(); err != nil {
		return fmt.Errorf("kolostore: %w", err)
	}
	return nil
}

func (p *pebbleBackend) NewBatch() Batch {
	return &pebbleBatch{p: p}
}

func (p *pebbleBackend) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	defer releaseDir(p.dir)
	if err := p.db.Close(); err != nil {
		return fmt.Errorf("kolostore: %w", err)
	}
	return nil
}

type pebbleBatch struct {
	ops
	p *pebbleBackend
}

func (b *pebbleBatch) Commit() error {
	if b.used {
		return errBatchUsed
	}
	p := b.p
	p.mu.RLock()
	defer p.mu.RUnlock()
	if p.closed {
		return ErrClosed
	}
	b.used = true
	size := uint64(pebbleBatchHeader)
	for _, o := range b.list {
		size += batchCost(o.key, o.value)
	}
	if size >= pebbleBatchLimit {
		return fmt.Errorf("%w: %d bytes, Pebble's limit is %d", ErrTooLarge, size, pebbleBatchLimit)
	}
	pb := p.db.NewBatchWithSize(int(size))
	defer pb.Close()
	for _, o := range b.list {
		var err error
		switch o.kind {
		case opPut:
			err = pb.Set(o.key, o.value, nil)
		case opDelete:
			err = pb.Delete(o.key, nil)
		case opDeleteRange:
			if bytes.Compare(o.key, o.value) < 0 {
				err = pb.DeleteRange(o.key, o.value, nil)
			}
		}
		if err != nil {
			return fmt.Errorf("kolostore: %w", err)
		}
	}
	b.list = nil
	if err := pb.Commit(pebble.Sync); err != nil {
		return fmt.Errorf("kolostore: %w", err)
	}
	return nil
}
