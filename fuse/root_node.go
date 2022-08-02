package fuse

import (
	"context"
	"log"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/superfly/litefs"
)

const RootInode = 1

var _ fs.Node = (*RootNode)(nil)
var _ fs.NodeStringLookuper = (*RootNode)(nil)
var _ fs.NodeCreater = (*RootNode)(nil)
var _ fs.NodeRemover = (*RootNode)(nil)
var _ fs.NodeFsyncer = (*RootNode)(nil)

// RootNode represents the root directory of the FUSE mount.
type RootNode struct {
	fsys *FileSystem
}

func newRootNode(fsys *FileSystem) *RootNode {
	return &RootNode{fsys: fsys}
}

func (n *RootNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = RootInode
	attr.Mode = os.ModeDir | 0777
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
}

func (n *RootNode) Lookup(ctx context.Context, name string) (fs.Node, error) {
	switch name {
	case PrimaryFilename:
		return n.lookupPrimaryNode(ctx)
	default:
		return n.lookupDBNode(ctx, name)
	}
}

func (n *RootNode) lookupPrimaryNode(ctx context.Context) (fs.Node, error) {
	primaryURL := n.fsys.store.PrimaryURL()
	if primaryURL == "" {
		return nil, syscall.ENOENT
	}
	return newPrimaryNode(n.fsys), nil
}

func (n *RootNode) lookupDBNode(ctx context.Context, name string) (fs.Node, error) {
	dbName, fileType := ParseFilename(name)

	db := n.fsys.store.DBByName(dbName)
	if db == nil {
		return nil, fuse.ToErrno(syscall.ENOENT)
	}

	switch fileType {
	case litefs.FileTypeDatabase:
		return newDatabaseNode(n.fsys, db), nil
	case litefs.FileTypeJournal:
		if _, err := os.Stat(db.JournalPath()); os.IsNotExist(err) {
			return nil, fuse.ToErrno(syscall.ENOENT)
		} else if err != nil {
			return nil, err
		}
		return newJournalNode(n.fsys, db), nil
	case litefs.FileTypeWAL, litefs.FileTypeSHM:
		return nil, fuse.ToErrno(syscall.ENOENT)
	default:
		return nil, fuse.ToErrno(syscall.ENOSYS)
	}
}

func (n *RootNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	dbName, fileType := ParseFilename(req.Name)

	switch fileType {
	case litefs.FileTypeDatabase:
		return n.createDatabase(ctx, dbName, req, resp)
	case litefs.FileTypeJournal:
		return n.createJournal(ctx, dbName, req, resp)
	default:
		return nil, nil, fuse.ToErrno(syscall.ENOSYS)
	}
}

func (n *RootNode) createDatabase(ctx context.Context, dbName string, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	db, file, err := n.fsys.store.CreateDB(dbName)
	if err == litefs.ErrDatabaseExists {
		return nil, nil, fuse.Errno(syscall.EEXIST)
	} else if err != nil {
		log.Printf("fuse: create(): cannot create database: %s", err)
		return nil, nil, toErrno(err)
	}

	node := newDatabaseNode(n.fsys, db)
	return node, &DatabaseHandle{node: node, file: file}, nil
}

func (n *RootNode) createJournal(ctx context.Context, dbName string, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	db := n.fsys.store.DBByName(dbName)
	if db == nil {
		log.Printf("fuse: create(): cannot create journal, database not found: %s", dbName)
		return nil, nil, fuse.Errno(syscall.ENOENT)
	}

	file, err := db.CreateJournal()
	if err != nil {
		log.Printf("fuse: create(): cannot create journal: %s", err)
		return nil, nil, toErrno(err)
	}

	node := newJournalNode(n.fsys, db)
	return node, newJournalHandle(node, file), nil
}

func (n *RootNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (n *RootNode) Remove(ctx context.Context, req *fuse.RemoveRequest) error {
	dbName, fileType := ParseFilename(req.Name)

	db := n.fsys.store.DBByName(dbName)
	if db == nil {
		return fuse.ToErrno(syscall.ENOENT)
	}

	switch fileType {
	case litefs.FileTypeJournal:
		return db.CommitJournal(litefs.JournalModeDelete)
	default:
		return fuse.ToErrno(syscall.ENOSYS)
	}
}

var _ fs.Handle = (*RootHandle)(nil)

//var _ fs.HandleFlusher = (*RootHandle)(nil)
//var _ fs.HandleReadAller = (*RootHandle)(nil)
//var _ fs.HandleReadDirAller = (*RootHandle)(nil)
//var _ fs.HandleReader = (*RootHandle)(nil)
//var _ fs.HandleWriter = (*RootHandle)(nil)
//var _ fs.HandleReleaser = (*RootHandle)(nil)

// RootHandle represents a directory handle for the root directory.
type RootHandle struct {
	id     uint64
	offset int
}

// NewRootHandle returns a new instance of RootHandle.
func NewRootHandle(id uint64) *RootHandle {
	return &RootHandle{id: id}
}

// ID returns the file handle identifier.
func (h *RootHandle) ID() uint64 { return h.id }
