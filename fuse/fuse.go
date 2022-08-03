package fuse

import (
	"fmt"
	"math"
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

// FileTypeInode returns the inode offset for the file type.
func FileTypeInode(t litefs.FileType) uint64 {
	switch t {
	case litefs.FileTypeDatabase:
		return 0
	case litefs.FileTypeJournal:
		return 1
	case litefs.FileTypeWAL:
		return 2
	case litefs.FileTypeSHM:
		return 3
	default:
		panic(fmt.Sprintf("FileTypeInode(): invalid file type: %d", t))
	}
}

// FileTypeFromInode returns the file type for the given inode offset.
func FileTypeFromInode(ino uint64) (litefs.FileType, error) {
	switch ino {
	case 0:
		return litefs.FileTypeDatabase, nil
	case 1:
		return litefs.FileTypeJournal, nil
	case 2:
		return litefs.FileTypeWAL, nil
	case 3:
		return litefs.FileTypeSHM, nil
	default:
		return litefs.FileTypeNone, fmt.Errorf("invalid inode file type: %d", ino)
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

// ParseInode parses an inode into its database ID & file type parts.
func ParseInode(ino uint64) (dbID uint32, fileType litefs.FileType, err error) {
	if ino < 1<<4 {
		return 0, 0, fmt.Errorf("invalid inode, out of range: %d", ino)
	}

	dbID64 := ino >> 4
	if dbID64 > math.MaxUint32 {
		return 0, 0, fmt.Errorf("inode overflows database id")
	}

	fileType, err = FileTypeFromInode(ino & 0xF)
	if err != nil {
		return 0, 0, err
	}
	return uint32(dbID64), fileType, nil
}

// toErrno converts an error to a FUSE status code.
func toErrno(err error) error {
	if err == nil {
		return nil
	} else if os.IsNotExist(err) {
		return fuse.Errno(syscall.ENOENT)
	} else if err == litefs.ErrReadOnlyReplica {
		return fuse.Errno(syscall.EROFS)
	}
	return fuse.Errno(syscall.EIO)
}

const (
	// RootNodeID is the identifier of the top-level directory.
	RootNodeID = 1

	// PrimaryNodeID is the identifier of the ".primary" file.
	PrimaryNodeID = 2
)

const (
	// PrimaryFilename is the name of the file that holds the current primary
	PrimaryFilename = ".primary"
)

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
