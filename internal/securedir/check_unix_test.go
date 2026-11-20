//go:build unix

package securedir

import (
	"errors"
	"os"
	"syscall"
	"testing"
	"time"
)

type fakeDir struct {
	mode os.FileMode
	uid  uint32
}

func (f fakeDir) Name() string       { return "dir" }
func (f fakeDir) Size() int64        { return 0 }
func (f fakeDir) Mode() os.FileMode  { return os.ModeDir | f.mode }
func (f fakeDir) ModTime() time.Time { return time.Time{} }
func (f fakeDir) IsDir() bool        { return true }
func (f fakeDir) Sys() any           { return &syscall.Stat_t{Uid: f.uid} }

var errInsecure = errors.New("insecure")

func TestCheckOwner(t *testing.T) {
	me := uint32(os.Geteuid())
	if err := Check("dir", fakeDir{0o700, me}, errInsecure); err != nil {
		t.Fatalf("own 0700 directory: %v", err)
	}
	for _, d := range []fakeDir{{0o700, me + 1}, {0o750, me}, {0o707, me}} {
		if err := Check("dir", d, errInsecure); !errors.Is(err, errInsecure) {
			t.Errorf("Check(mode %v, uid %d): %v, want errInsecure", d.mode, d.uid, err)
		}
	}
}
