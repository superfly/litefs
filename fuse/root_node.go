package fuse

import (
	"context"
	"log"
	"os"
	"sync"
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
	mu    sync.Mutex
	fsys  *FileSystem
	nodes map[string]fs.Node // nodes by name
}

// newRootNode returns a new instance of RootNode.
func newRootNode(fsys *FileSystem) *RootNode {
	return &RootNode{
		fsys:  fsys,
		nodes: make(map[string]fs.Node),
	}
}

// Node returns a child node by filename. Returns nil if it does not exist.
func (n *RootNode) Node(name string) fs.Node {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.nodes[name]
}

// Attr returns the attributes for the root directory.
func (n *RootNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Inode = RootInode
	attr.Mode = os.ModeDir | 0777
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
}

// Lookup returns a node for a file in the root directory.
func (n *RootNode) Lookup(ctx context.Context, name string) (node fs.Node, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Check if we've already seen this node.
	if node = n.nodes[name]; node != nil {
		return node, nil
	}

	switch name {
	case PrimaryFilename:
		if node, err = n.lookupPrimaryNode(ctx); err != nil {
			return nil, err
		}
	default:
		if node, err = n.lookupDBNode(ctx, name); err != nil {
			return nil, err
		}
	}

	// Cache node on successful lookup.
	n.nodes[name] = node

	return node, nil
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
			return nil, fuse.ENOENT
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

func (n *RootNode) Create(ctx context.Context, req *fuse.CreateRequest, resp *fuse.CreateResponse) (node fs.Node, h fs.Handle, err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	dbName, fileType := ParseFilename(req.Name)

	switch fileType {
	case litefs.FileTypeDatabase:
		if node, h, err = n.createDatabase(ctx, dbName, req, resp); err != nil {
			return nil, nil, err
		}
	case litefs.FileTypeJournal:
		if node, h, err = n.createJournal(ctx, dbName, req, resp); err != nil {
			return nil, nil, err
		}
	default:
		return nil, nil, fuse.ToErrno(syscall.ENOSYS)
	}

	// Cache node on creation.
	n.nodes[req.Name] = node

	return node, h, nil
}

func (n *RootNode) createDatabase(ctx context.Context, dbName string, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	db, file, err := n.fsys.store.CreateDB(dbName)
	if err == litefs.ErrDatabaseExists {
		return nil, nil, fuse.Errno(syscall.EEXIST)
	} else if err != nil {
		log.Printf("fuse: create(): cannot create database: %s", err)
		return nil, nil, ToError(err)
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
		return nil, nil, ToError(err)
	}

	node := newJournalNode(n.fsys, db)
	return node, newJournalHandle(node, file), nil
}

// Fsync is a no-op as directory sync is handled by the file.
// This is required as the database files are grouped by database internally.
func (n *RootNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

// Remove deletes the file from disk. This is only supported on the journal file currently.
func (n *RootNode) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	n.mu.Lock()
	defer n.mu.Unlock()

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

// ForgetNode removes the node from the node map.
func (n *RootNode) ForgetNode(node fs.Node) {
	n.mu.Lock()
	defer n.mu.Unlock()
	for k, v := range n.nodes {
		if v == node {
			delete(n.nodes, k)
		}
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
