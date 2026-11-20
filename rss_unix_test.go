//go:build unix

package kolostore

import (
	"os"
	"runtime"
	"syscall"
)

// maxRSS returns the peak resident size of an ended process in bytes, or 0 if unknown.
func maxRSS(ps *os.ProcessState) uint64 {
	ru, ok := ps.SysUsage().(*syscall.Rusage)
	if !ok {
		return 0
	}
	if runtime.GOOS == "darwin" {
		return uint64(ru.Maxrss)
	}
	return uint64(ru.Maxrss) << 10
}
