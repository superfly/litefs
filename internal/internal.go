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
	defer func() { _ = f.Close() }()

	if err := f.Sync(); err != nil {
		return err
	}
	return f.Close()
}
