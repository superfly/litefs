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
	path  string // mount path
	store *litefs.Store

	conn   *fuse.Conn
	server *fs.Server
	root   *RootNode

	// If true, allows other users to access the FUSE mount.
	// Must set "user_allow_other" option in /etc/fuse.conf as well.
	AllowOther bool

	// User & Group ID for all files in the filesystem.
	Uid int
	Gid int

	// If true, enables debug logging.
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
	// Attempt to unmount if it did not close cleanly before.
	_ = fuse.Unmount(fsys.path)

	// Ensure mount directory exists before trying to mount to it.
	if err := os.MkdirAll(fsys.path, 0777); err != nil {
		return err
	}

	options := []fuse.MountOption{
		fuse.FSName("litefs"),
		fuse.LockingPOSIX(),
		fuse.ExplicitInvalidateData(),
	}
	if fsys.AllowOther {
		options = append(options, fuse.AllowOther())
	}

	fsys.conn, err = fuse.Mount(fsys.path, options...)
	if err != nil {
		return err
	}

	var config fs.Config
	if fsys.Debug {
		config.Debug = fsys.debugFn
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
	if fsys.conn != nil {
		if e := fuse.Unmount(fsys.path); err == nil {
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

// InvalidateDB invalidates the entire database from the kernel page cache.
func (fsys *FileSystem) InvalidateDB(db *litefs.DB) error {
	node := fsys.root.Node(db.Name())
	if node == nil {
		return nil
	}

	if err := fsys.server.InvalidateNodeData(node); err != nil && err != fuse.ErrNotCached {
		return err
	}
	return nil
}

// InvalidateDBRange invalidates a database in the kernel page cache.
func (fsys *FileSystem) InvalidateDBRange(db *litefs.DB, offset, size int64) error {
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

// InvalidateEntry removes the file from the cache.
func (fsys *FileSystem) InvalidateEntry(name string) error {
	if err := fsys.server.InvalidateEntry(fsys.root, name); err != nil && err != fuse.ErrNotCached {
		return err
	}
	return nil
}

// debugFn is called by the underlying FUSE library when debug logging is enabled.
func (fsys *FileSystem) debugFn(msg any) {
	status := "r"
	if fsys.store.IsPrimary() {
		status = "p"
	}
	log.Printf("%s [%s]: %s", fsys.store.ID(), status, msg)
}
