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

var _ fs.FS = (*FileSystem)(nil)
var _ fs.FSStatfser = (*FileSystem)(nil)
var _ litefs.Invalidator = (*FileSystem)(nil)

// FileSystem represents a raw interface to the FUSE file system.
type FileSystem struct {
	path string // mount path

	store *litefs.Store

	conn   *fuse.Conn
	server *fs.Server
	root   *RootNode

	// User & Group ID for all files in the filesystem.
	Uid int
	Gid int

	// If true, logs debug information about every FUSE call.
	Debug bool
}

// NewFileSystem returns a new instance of FileSystem.
func NewFileSystem(path string, store *litefs.Store) *FileSystem {
	fsys := &FileSystem{
		path:  path,
		store: store,

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
	fsys.conn, err = fuse.Mount(fsys.path,
		fuse.FSName("litefs"),
		fuse.LockingPOSIX(),
	)
	if err != nil {
		return err
	}

	var config fs.Config
	if fsys.Debug {
		config.Debug = func(msg interface{}) { log.Print(msg) }
	}

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
	if e := fuse.Unmount(fsys.path); err == nil {
		err = e
	}

	if fsys.conn != nil {
		if e := fsys.conn.Close(); err == nil {
			err = e
		}
	}
	return err
}

// Root returns the root directory in the file system.
func (fsys *FileSystem) Root() (fs.Node, error) {
	return fsys.root, nil
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
