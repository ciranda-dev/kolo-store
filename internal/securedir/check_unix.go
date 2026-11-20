//go:build unix

package securedir

import (
	"fmt"
	"os"
	"syscall"
)

// Check returns an error wrapping insecure if group or others can access fi or another user owns it.
func Check(dir string, fi os.FileInfo, insecure error) error {
	if perm := fi.Mode().Perm(); perm&0o077 != 0 {
		return fmt.Errorf("%w: %s has mode %v", insecure, dir, perm)
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); !ok || int(st.Uid) != os.Geteuid() {
		return fmt.Errorf("%w: %s is not owned by this user", insecure, dir)
	}
	return nil
}
