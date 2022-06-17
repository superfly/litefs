package litefs

import (
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

// Root represents the FUSE root.
type Root struct {
	*fs.LoopbackRoot
	store *Store
}

// NewRoot returns a new instance of Root.
func NewRoot(path string, dev uint64, store *Store) *Root {
	root := &Root{
		LoopbackRoot: &fs.LoopbackRoot{
			Path: path,
			Dev:  dev,
		},
		store: store,
	}

	root.NewNode = root.newNode
	return root
}

// newNode provides an injection mechanism to add our own Node type.
func (r *Root) newNode(lpRoot *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	dbName, fileType := ParseFilename(name)
	db := r.store.FindDBByName(dbName)

	return &Node{
		LoopbackNode: &fs.LoopbackNode{RootData: lpRoot},
		root:         r,
		name:         name,
		db:           db,
		fileType:     fileType,
	}
}

// FileType represents a type of SQLite file.
type FileType int

const (
	// Main database file
	FileTypeDatabase = FileType(iota)

	// Rollback journal
	FileTypeJournal

	// Write-ahead log
	FileTypeWAL

	// Shared memory
	FileTypeSHM
)

// ParseFilename parses a base name into database name & file type parts.
func ParseFilename(name string) (dbName string, fileType FileType) {
	if strings.HasSuffix(name, "-journal") {
		return strings.TrimSuffix(name, "-journal"), FileTypeJournal
	} else if strings.HasSuffix(name, "-wal") {
		return strings.TrimSuffix(name, "-wal"), FileTypeWAL
	} else if strings.HasSuffix(name, "-shm") {
		return strings.TrimSuffix(name, "-shm"), FileTypeSHM
	}
	return name, FileTypeDatabase
}
