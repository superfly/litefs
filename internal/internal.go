package internal

import (
	"os"
)

// Sync performs an fsync on the given path. Typically used for directories.
func Sync(path string) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
