package fuse

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"bazil.org/fuse"
	"github.com/superfly/litefs"
)

// FileTypeFilename returns the base name for the internal data file.
func FileTypeFilename(t litefs.FileType) string {
	switch t {
	case litefs.FileTypeDatabase:
		return "database"
	case litefs.FileTypeJournal:
		return "journal"
	case litefs.FileTypeWAL:
		return "wal"
	case litefs.FileTypeSHM:
		return "shm"
	case litefs.FileTypePos:
		return "pos"
	default:
		panic(fmt.Sprintf("FileTypeFilename(): invalid file type: %d", t))
	}
}

// ParseFilename parses a base name into database name & file type parts.
func ParseFilename(name string) (dbName string, fileType litefs.FileType) {
	if strings.HasSuffix(name, "-journal") {
		return strings.TrimSuffix(name, "-journal"), litefs.FileTypeJournal
	} else if strings.HasSuffix(name, "-wal") {
		return strings.TrimSuffix(name, "-wal"), litefs.FileTypeWAL
	} else if strings.HasSuffix(name, "-shm") {
		return strings.TrimSuffix(name, "-shm"), litefs.FileTypeSHM
	} else if strings.HasSuffix(name, "-pos") {
		return strings.TrimSuffix(name, "-pos"), litefs.FileTypePos
	}
	return name, litefs.FileTypeDatabase
}

// ToError converts an error to a wrapped error with a FUSE status code.
func ToError(err error) error {
	if os.IsNotExist(err) {
		return &Error{err: err, errno: fuse.ToErrno(syscall.ENOENT)}
	} else if err == litefs.ErrReadOnlyReplica {
		return &Error{err: err, errno: fuse.ToErrno(syscall.EACCES)}
	}
	return err
}

// Error wraps an error to return a "No Entry" FUSE error.
type Error struct {
	err   error
	errno fuse.Errno
}

func (e *Error) Errno() fuse.Errno { return e.errno }
func (e *Error) Error() string     { return e.err.Error() }
