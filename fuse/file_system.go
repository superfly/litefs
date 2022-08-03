package fuse

import (
	"log"
	"os"
	"sync"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/superfly/litefs"
)

var _ fs.FS = (*FileSystem)(nil)
var _ litefs.Invalidator = (*FileSystem)(nil)

// FileSystem represents a raw interface to the FUSE file system.
type FileSystem struct {
	mu   sync.Mutex
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

// InvalidateDB invalidates a database in the kernel page cache.
func (fsys *FileSystem) InvalidateDB(db *litefs.DB) error {
	if err := fsys.conn.InvalidateEntry(fuse.NodeID(RootInode), db.Name()); err != nil && err != fuse.ErrNotCached {
		return err
	}
	return nil
}
