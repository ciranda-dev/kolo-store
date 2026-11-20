//go:build !unix

package kolostore

import "os"

// maxRSS is unknown on this platform.
func maxRSS(*os.ProcessState) uint64 { return 0 }
