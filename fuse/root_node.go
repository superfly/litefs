package fuse

import (
	"context"
	"log"
	"os"
	"sort"
	"sync"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/superfly/litefs"
)

const RootInode = 1

var _ fs.Node = (*RootNode)(nil)
var _ fs.NodeStringLookuper = (*RootNode)(nil)
var _ fs.NodeOpener = (*RootNode)(nil)
var _ fs.NodeCreater = (*RootNode)(nil)
var _ fs.NodeRemover = (*RootNode)(nil)
var _ fs.NodeFsyncer = (*RootNode)(nil)
var _ fs.NodeListxattrer = (*RootNode)(nil)
var _ fs.NodeGetxattrer = (*RootNode)(nil)
var _ fs.NodeSetxattrer = (*RootNode)(nil)
var _ fs.NodeRemovexattrer = (*RootNode)(nil)
var _ fs.NodePoller = (*RootNode)(nil)
var _ fs.Handle = (*RootNode)(nil)
var _ fs.HandleReadDirAller = (*RootNode)(nil)

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

	if n.fsys.store.IsPrimary() {
		attr.Mode = os.ModeDir | 0777
	} else {
		attr.Mode = os.ModeDir | 0555
	}

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
	info := n.fsys.store.PrimaryInfo()
	if info == nil {
		return nil, syscall.ENOENT
	}
	return newPrimaryNode(n.fsys), nil
}

func (n *RootNode) lookupDBNode(ctx context.Context, name string) (fs.Node, error) {
	dbName, fileType := ParseFilename(name)

	db := n.fsys.store.DB(dbName)
	if db == nil {
		return nil, fuse.ToErrno(syscall.ENOENT)
	}

	switch fileType {
	case litefs.FileTypeDatabase:
		file, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666)
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		} else if err != nil {
			return nil, err
		}
		return newDatabaseNode(n.fsys, db, file), nil
	case litefs.FileTypeJournal:
		file, err := os.OpenFile(db.JournalPath(), os.O_RDWR, 0666)
		if os.IsNotExist(err) {
			return nil, fuse.ENOENT
		} else if err != nil {
			return nil, err
		}
		return newJournalNode(n.fsys, db, file), nil
	case litefs.FileTypeWAL, litefs.FileTypeSHM:
		return nil, fuse.ToErrno(syscall.ENOENT)
	case litefs.FileTypePos:
		return newPosNode(n.fsys, db), nil
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

	node := newDatabaseNode(n.fsys, db, file)
	return node, node, nil
}

func (n *RootNode) createJournal(ctx context.Context, dbName string, req *fuse.CreateRequest, resp *fuse.CreateResponse) (fs.Node, fs.Handle, error) {
	db := n.fsys.store.DB(dbName)
	if db == nil {
		log.Printf("fuse: create(): cannot create journal, database not found: %s", dbName)
		return nil, nil, fuse.Errno(syscall.ENOENT)
	}

	file, err := db.CreateJournal()
	if err != nil {
		log.Printf("fuse: create(): cannot create journal: %s", err)
		return nil, nil, ToError(err)
	}

	node := newJournalNode(n.fsys, db, file)
	return node, node, nil
}

// Fsync is a no-op as directory sync is handled by the file.
// This is required as the database files are grouped by database internally.
func (n *RootNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return nil
}

func (n *RootNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}

// Remove deletes the file from disk. This is only supported on the journal file currently.
func (n *RootNode) Remove(ctx context.Context, req *fuse.RemoveRequest) (err error) {
	dbName, fileType := ParseFilename(req.Name)

	db := n.fsys.store.DB(dbName)
	if db == nil {
		return fuse.ToErrno(syscall.ENOENT)
	}

	switch fileType {
	case litefs.FileTypeJournal:
		if err := db.CommitJournal(litefs.JournalModeDelete); err != nil {
			log.Printf("fuse: commit error: %s", err)
			return err
		}
		return nil
	default:
		return fuse.ToErrno(syscall.ENOSYS)
	}
}

func (n *RootNode) ReadDirAll(ctx context.Context) (ents []fuse.Dirent, err error) {
	// Show ".primary" file if this is a replica currently connected to the primary.
	if info := n.fsys.store.PrimaryInfo(); info != nil {
		ents = append(ents, fuse.Dirent{
			Name: PrimaryFilename,
			Type: fuse.DT_File,
		})
	}

	// Return a list of database files.
	dbs := n.fsys.store.DBs()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })

	for _, db := range dbs {
		ents = append(ents, fuse.Dirent{
			Name: db.Name(),
			Type: fuse.DT_File,
		})
	}

	return ents, nil
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

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *RootNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *RootNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *RootNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *RootNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *RootNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}
