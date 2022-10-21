package fuse

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/superfly/litefs"
)

var _ fs.Node = (*DatabaseNode)(nil)
var _ fs.NodeOpener = (*DatabaseNode)(nil)
var _ fs.NodeFsyncer = (*DatabaseNode)(nil)
var _ fs.NodeForgetter = (*DatabaseNode)(nil)
var _ fs.NodeListxattrer = (*DatabaseNode)(nil)
var _ fs.NodeGetxattrer = (*DatabaseNode)(nil)
var _ fs.NodeSetxattrer = (*DatabaseNode)(nil)
var _ fs.NodeRemovexattrer = (*DatabaseNode)(nil)
var _ fs.NodePoller = (*DatabaseNode)(nil)

// DatabaseNode represents a SQLite database file.
type DatabaseNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newDatabaseNode(fsys *FileSystem, db *litefs.DB) *DatabaseNode {
	return &DatabaseNode{
		fsys: fsys,
		db:   db,
	}
}

func (n *DatabaseNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := os.Stat(n.db.DatabasePath())
	if os.IsNotExist(err) {
		return fuse.ENOENT
	} else if err != nil {
		return err
	}

	if n.db.Store().IsPrimary() {
		attr.Mode = 0666
	} else {
		attr.Mode = 0444
	}

	attr.Size = uint64(fi.Size())
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	return nil
}

func (n *DatabaseNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		if err := os.Truncate(n.db.DatabasePath(), int64(req.Size)); err != nil {
			return err
		}
	}
	return n.Attr(ctx, &resp.Attr)
}

func (n *DatabaseNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f, err := os.OpenFile(n.db.DatabasePath(), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return newDatabaseHandle(n, f), nil
}

func (n *DatabaseNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f, err := os.Open(n.db.DatabasePath())
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// TODO: fsync parent directory
	return nil
}

func (n *DatabaseNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *DatabaseNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *DatabaseNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *DatabaseNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *DatabaseNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *DatabaseNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}

var _ fs.Handle = (*DatabaseHandle)(nil)
var _ fs.HandleReader = (*DatabaseHandle)(nil)
var _ fs.HandleWriter = (*DatabaseHandle)(nil)
var _ fs.HandlePOSIXLocker = (*DatabaseHandle)(nil)

// DatabaseHandle represents a file handle to a SQLite database file.
type DatabaseHandle struct {
	node *DatabaseNode
	file *os.File
}

func newDatabaseHandle(node *DatabaseNode, file *os.File) *DatabaseHandle {
	return &DatabaseHandle{node: node, file: file}
}

func (h *DatabaseHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.file.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = buf[:n]
	return err
}

func (h *DatabaseHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if err := h.node.db.WriteDatabase(h.file, req.Data, req.Offset); err != nil {
		log.Printf("fuse: write(): database error: %s", err)
		return err
	}
	resp.Size = len(req.Data)
	return nil
}

func (h *DatabaseHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	if gs := h.node.fsys.GuardSet(h.node.db, req.LockOwner); gs != nil {
		gs.UnlockDatabase()
	}
	return nil
}

func (h *DatabaseHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.file.Close()
}

func (h *DatabaseHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	// Parse lock range and ensure we are only performing one lock at a time.
	lockTypes := litefs.ParseDatabaseLockRange(req.Lock.Start, req.Lock.End)
	if len(lockTypes) == 0 {
		return fmt.Errorf("no database locks")
	} else if len(lockTypes) > 1 {
		return fmt.Errorf("cannot acquire multiple locks at once")
	}
	lockType := lockTypes[0]

	guard := h.node.fsys.CreateGuardSetIfNotExists(h.node.db, req.LockOwner).Guard(lockType)

	switch typ := req.Lock.Type; typ {
	case fuse.LockRead:
		if !guard.TryRLock() {
			return syscall.EAGAIN
		}
		return nil
	case fuse.LockWrite:
		if !guard.TryLock() {
			return syscall.EAGAIN
		}
		return nil
	default:
		panic("fuse.DatabaseNode.lock(): invalid POSIX lock type")
	}
}

func (h *DatabaseHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	return fuse.Errno(syscall.ENOSYS)
}

func (h *DatabaseHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	for _, lockType := range litefs.ParseDatabaseLockRange(req.Lock.Start, req.Lock.End) {
		if gs := h.node.fsys.GuardSet(h.node.db, req.LockOwner); gs != nil {
			gs.Guard(lockType).Unlock()
		}
	}
	return nil
}

func (h *DatabaseHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	for _, lockType := range litefs.ParseDatabaseLockRange(req.Lock.Start, req.Lock.End) {
		if !h.canLock(req.LockOwner, req.Lock.Type, lockType) {
			resp.Lock = fuse.FileLock{
				Start: req.Lock.Start,
				End:   req.Lock.End,
				Type:  fuse.LockWrite,
				PID:   -1,
			}
			return nil
		}
	}
	return nil
}

func (h *DatabaseHandle) canLock(owner fuse.LockOwner, typ fuse.LockType, lockType litefs.LockType) bool {
	guard := h.node.fsys.CreateGuardSetIfNotExists(h.node.db, owner).Guard(lockType)
	switch typ {
	case fuse.LockUnlock:
		return true
	case fuse.LockRead:
		return guard.CanRLock()
	case fuse.LockWrite:
		v, _ := guard.CanLock()
		return v
	default:
		panic("fuse.DatabaseHandle.canLock(): invalid POSIX lock type")
	}
}
