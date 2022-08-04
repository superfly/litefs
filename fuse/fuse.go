package fuse

import (
	"fmt"
	"os"
	"strings"
	"syscall"

	"bazil.org/fuse"
	"github.com/superfly/litefs"
)

//----------------------------------
/*
func (fsys *FileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	h := fs.RootHandle(input.Fh)
	if h == nil {
		log.Printf("fuse: readdirplus(): bad file handle: %d", input.Fh)
		return fuse.EBADF
	}

	// Build hidden, fixed entries list first.
	var entries []fuse.DirEntry
	if primaryURL := fs.store.PrimaryURL(); primaryURL != "" {
		entries = append(entries, fuse.DirEntry{
			Name: PrimaryFilename,
			Ino:  PrimaryNodeID,
			Mode: 0100444,
		})
	}

	// Write out entries until we fill the buffer.
	var index int
	for _, entry := range entries {
		if index < h.offset {
			continue
		}

		if out.AddDirLookupEntry(entry) == nil {
			return fuse.OK
		}
		index++
		h.offset++
	}

	// Read & sort list of databases from the store.
	dbs := fs.store.DBs()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })

	// Iterate over databases starting from the offset.
	for _, db := range dbs {
		if index < h.offset {
			continue
		}

		// Write the entry to the buffer; if nil returned then buffer is full.
		if out.AddDirLookupEntry(fuse.DirEntry{
			Name: db.Name(),
			Ino:  fs.dbIno(db.ID(), litefs.FileTypeDatabase),
			Mode: 0100666},
		) == nil {
			break
		}

		index++
		h.offset++
	}
	return fuse.OK
}
*/

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
	}
	return name, litefs.FileTypeDatabase
}

// ToError converts an error to a wrapped error with a FUSE status code.
func ToError(err error) error {
	if os.IsNotExist(err) {
		return &Error{err: err, errno: fuse.ENOENT}
	} else if err == litefs.ErrReadOnlyReplica {
		return &Error{err: err, errno: fuse.Errno(syscall.EROFS)}
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

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
