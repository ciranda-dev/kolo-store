//go:build !unix

package securedir

import "os"

// Check does nothing where permissions and owner are not checked.
func Check(string, os.FileInfo, error) error { return nil }
