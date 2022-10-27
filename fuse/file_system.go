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

var _ fs.FS = (*FileSystem)(nil)
var _ fs.FSStatfser = (*FileSystem)(nil)
var _ litefs.Invalidator = (*FileSystem)(nil)

// FileSystem represents a raw interface to the FUSE file system.
type FileSystem struct {
	mu sync.Mutex

	path  string // mount path
	store *litefs.Store

	conn   *fuse.Conn
	server *fs.Server
	root   *RootNode

	guardSets map[fuse.LockOwner]*litefs.GuardSet

	// User & Group ID for all files in the filesystem.
	Uid int
	Gid int

	// If set, function is called for each FUSE request & response.
	Debug func(msg any)
}

// NewFileSystem returns a new instance of FileSystem.
func NewFileSystem(path string, store *litefs.Store) *FileSystem {
	fsys := &FileSystem{
		path:  path,
		store: store,

		guardSets: make(map[fuse.LockOwner]*litefs.GuardSet),

		Uid: os.Getuid(),
		Gid: os.Getgid(),
	}

	fsys.root = newRootNode(fsys)

	return fsys
}

// Path returns the path to the mount point.
func (fsys *FileSystem) Path() string { return fsys.path }

// Store returns the underlying store.
func (fsys *FileSystem) Store() *litefs.Store { return fsys.store }

// Mount mounts the file system to the mount point.
func (fsys *FileSystem) Mount() (err error) {
	// Ensure mount directory exists before trying to mount to it.
	if err := os.MkdirAll(fsys.path, 0777); err != nil {
		return err
	}

	fsys.conn, err = fuse.Mount(fsys.path,
		fuse.FSName("litefs"),
		fuse.LockingPOSIX(),
	)
	if err != nil {
		return err
	}

	config := fs.Config{Debug: fsys.Debug}
	fsys.server = fs.New(fsys.conn, &config)

	go func() {
		if err := fsys.server.Serve(fsys); err != nil {
			log.Printf("fuse serve error: %s", err)
		}
	}()

	return nil
}

// Unmount unmounts the file system.
func (fsys *FileSystem) Unmount() (err error) {
	if fsys.conn != nil {
		if e := fuse.Unmount(fsys.path); err == nil {
			err = e
		}
		if e := fsys.conn.Close(); err == nil {
			err = e
		}
		fsys.conn = nil
	}
	return err
}

// Root returns the root directory in the file system.
func (fsys *FileSystem) Root() (fs.Node, error) {
	return fsys.root, nil
}

// GuardSet returns a database guard set for the given owner.
func (fsys *FileSystem) GuardSet(db *litefs.DB, owner fuse.LockOwner) *litefs.GuardSet {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	gs := fsys.guardSets[owner]
	if gs == nil {
		gs = db.GuardSet()
		fsys.guardSets[owner] = gs
	}
	return gs
}

// CreateGuardSetIfNotExists returns a database guard set for the given owner.
// Creates a new guard set if one is not associated with the owner.
func (fsys *FileSystem) CreateGuardSetIfNotExists(db *litefs.DB, owner fuse.LockOwner) *litefs.GuardSet {
	fsys.mu.Lock()
	defer fsys.mu.Unlock()

	gs := fsys.guardSets[owner]
	if gs == nil {
		gs = db.GuardSet()
		fsys.guardSets[owner] = gs
	}
	return gs
}

// Statfs is a passthrough to the underlying file system.
func (fsys *FileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	// Obtain statfs() call from underlying store path.
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(fsys.store.Path(), &statfs); err != nil {
		return err
	}

	// Copy stats over from the underlying file system to the response.
	resp.Blocks = statfs.Blocks
	resp.Bfree = statfs.Bfree
	resp.Bavail = statfs.Bavail
	resp.Files = statfs.Files
	resp.Ffree = statfs.Ffree
	resp.Bsize = uint32(statfs.Bsize)
	resp.Namelen = uint32(statfs.Namelen)
	resp.Frsize = uint32(statfs.Frsize)

	return nil
}

// InvalidateDB invalidates a database in the kernel page cache.
func (fsys *FileSystem) InvalidateDB(db *litefs.DB, offset, size int64) error {
	node := fsys.root.Node(db.Name())
	if node == nil {
		return nil
	}

	if err := fsys.server.InvalidateNodeDataRange(node, offset, size); err != nil && err != fuse.ErrNotCached {
		return err
	}
	return nil
}

// InvalidateSHM invalidates the SHM file in the kernel page cache.
func (fsys *FileSystem) InvalidateSHM(db *litefs.DB) error {
	node := fsys.root.Node(db.Name() + "-shm")
	if node == nil {
		return nil
	}

	if err := fsys.server.InvalidateNodeData(node); err != nil && err != fuse.ErrNotCached {
		return err
	}
	return nil
}

// InvalidatePos invalidates the position file in the kernel page cache.
func (fsys *FileSystem) InvalidatePos(db *litefs.DB) error {
	node := fsys.root.Node(db.Name() + "-pos")
	if node == nil {
		return nil
	}

	if err := fsys.server.InvalidateNodeData(node); err != nil && err != fuse.ErrNotCached {
		return err
	}
	return nil
}
