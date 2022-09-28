package fuse

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"sync"
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
var _ fs.Handle = (*DatabaseNode)(nil)
var _ fs.HandleReader = (*DatabaseNode)(nil)
var _ fs.HandleWriter = (*DatabaseNode)(nil)
var _ fs.HandlePOSIXLocker = (*DatabaseNode)(nil)

// DatabaseNode represents a SQLite database file.
type DatabaseNode struct {
	mu        sync.Mutex
	fsys      *FileSystem
	db        *litefs.DB
	file      *os.File
	guardSets map[fuse.LockOwner]*litefs.GuardSet
}

func newDatabaseNode(fsys *FileSystem, db *litefs.DB, file *os.File) *DatabaseNode {
	return &DatabaseNode{
		fsys:      fsys,
		db:        db,
		file:      file,
		guardSets: make(map[fuse.LockOwner]*litefs.GuardSet),
	}
}

func (n *DatabaseNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := n.file.Stat()
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
	return nil
}

func (n *DatabaseNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}

func (n *DatabaseNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return n.file.Sync()
}

// lock tries to acquire a lock on a byte range of the node.
// If a conflicting lock is already held, returns syscall.EAGAIN.
func (n *DatabaseNode) Lock(ctx context.Context, req *fuse.LockRequest) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Parse lock range and ensure we are only performing one lock at a time.
	lockTypes := litefs.ParseLockRange(req.Lock.Start, req.Lock.End)
	if len(lockTypes) == 0 {
		return fmt.Errorf("no locks")
	} else if len(lockTypes) > 1 {
		return fmt.Errorf("cannot acquire multiple locks at once")
	}
	lockType := lockTypes[0]

	guard := n.guardSet(req.LockOwner).Guard(lockType)

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

func (n *DatabaseNode) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	return fuse.Errno(syscall.ENOSYS)
}

// Unlock releases the lock on a byte range of the node. Locks can
// be released also implicitly, see HandleFlockLocker and HandlePOSIXLocker.
func (n *DatabaseNode) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, lockType := range litefs.ParseLockRange(req.Lock.Start, req.Lock.End) {
		guard := n.guardSet(req.LockOwner).Guard(lockType)
		guard.Unlock()
	}
	return nil
}

// QueryLock returns the current state of locks held for the byte range of the node.
func (n *DatabaseNode) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, lockType := range litefs.ParseLockRange(req.Lock.Start, req.Lock.End) {
		if !n.canLock(req.LockOwner, req.Lock.Type, lockType) {
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

// canLock returns true if the given lock can be acquired.
func (n *DatabaseNode) canLock(owner fuse.LockOwner, typ fuse.LockType, lockType litefs.LockType) bool {
	guard := n.guardSet(owner).Guard(lockType)

	switch typ {
	case fuse.LockUnlock:
		return true
	case fuse.LockRead:
		return guard.CanRLock()
	case fuse.LockWrite:
		return guard.CanLock()
	default:
		panic("fuse.DatabaseNode.canLock(): invalid POSIX lock type")
	}
}

func (n *DatabaseNode) guardSet(owner fuse.LockOwner) *litefs.GuardSet {
	gs := n.guardSets[owner]
	if gs == nil {
		gs = n.db.GuardSet()
		n.guardSets[owner] = gs
	}
	return gs
}

func (n *DatabaseNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	nn, err := n.file.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = buf[:nn]
	return err
}

func (n *DatabaseNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if err := n.db.WriteDatabase(n.file, req.Data, req.Offset); err != nil {
		log.Printf("fuse: write(): database error: %s", err)
		return err
	}
	resp.Size = len(req.Data)
	return nil
}

func (n *DatabaseNode) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	guardSet := n.guardSets[req.LockOwner]
	if guardSet == nil {
		return nil
	}

	guardSet.Unlock()
	delete(n.guardSets, req.LockOwner)
	return nil
}

func (n *DatabaseNode) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	// return n.file.Close()
	return nil
}

func (n *DatabaseNode) Forget() {
	_ = n.file.Close()
	n.fsys.root.ForgetNode(n)
}

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
