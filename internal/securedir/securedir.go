// Package securedir checks that a store directory is private to the current user.
package securedir

import (
	"fmt"
	"os"
)

// Prepare creates dir with mode 0700 if it is missing, then returns its FileInfo after [Check].
func Prepare(dir string, insecure error) (os.FileInfo, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, fmt.Errorf("kolostore: %w", err)
	}
	fi, err := os.Stat(dir)
	if err != nil {
		return nil, fmt.Errorf("kolostore: %w", err)
	}
	if err := Check(dir, fi, insecure); err != nil {
		return nil, err
	}
	return fi, nil
}

// Recheck returns an error wrapping insecure unless dir is still the directory before and still passes [Check].
func Recheck(dir string, before os.FileInfo, insecure error) error {
	after, err := os.Stat(dir)
	if err != nil {
		return err
	}
	if !os.SameFile(before, after) {
		return fmt.Errorf("%w: %s changed while opening", insecure, dir)
	}
	return Check(dir, after, insecure)
}
