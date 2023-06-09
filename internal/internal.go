package internal

import (
	"io"
	"os"
	"strings"
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

// ReadFullAt is an implementation of io.ReadFull() but for io.ReaderAt.
func ReadFullAt(r io.ReaderAt, buf []byte, off int64) (n int, err error) {
	for n < len(buf) && err == nil {
		var nn int
		nn, err = r.ReadAt(buf[n:], off+int64(n))
		n += nn
	}
	if n >= len(buf) {
		return n, nil
	} else if n > 0 && err == io.EOF {
		return n, io.ErrUnexpectedEOF
	}
	return n, err
}

// Close closes closer but ignores select errors.
func Close(closer io.Closer) (err error) {
	if closer == nil {
		return nil
	} else if err = closer.Close(); err == nil {
		return nil
	}

	if strings.Contains(err.Error(), `use of closed network connection`) {
		return nil
	}
	if strings.Contains(err.Error(), `http: Server closed`) {
		return nil
	}
	return err
}
