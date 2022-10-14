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

var _ fs.Node = (*WALNode)(nil)
var _ fs.NodeOpener = (*WALNode)(nil)
var _ fs.NodeFsyncer = (*WALNode)(nil)
var _ fs.NodeForgetter = (*WALNode)(nil)
var _ fs.NodeListxattrer = (*WALNode)(nil)
var _ fs.NodeGetxattrer = (*WALNode)(nil)
var _ fs.NodeSetxattrer = (*WALNode)(nil)
var _ fs.NodeRemovexattrer = (*WALNode)(nil)
var _ fs.NodePoller = (*WALNode)(nil)

// WALNode represents a SQLite WAL file.
type WALNode struct {
	fsys *FileSystem
	db   *litefs.DB

	mu        sync.Mutex
	guardSets map[fuse.LockOwner]*litefs.WALGuardSet
}

func newWALNode(fsys *FileSystem, db *litefs.DB) *WALNode {
	return &WALNode{
		fsys: fsys,
		db:   db,

		guardSets: make(map[fuse.LockOwner]*litefs.WALGuardSet),
	}
}

func (n *WALNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := os.Stat(n.db.WALPath())
	if os.IsNotExist(err) {
		return fuse.ENOENT
	} else if err != nil {
		return err
	}

	attr.Mode = 0666
	attr.Size = uint64(fi.Size())
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	return nil
}

func (n *WALNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		if err := os.Truncate(n.db.WALPath(), int64(req.Size)); err != nil {
			return err
		}
	}
	return n.Attr(ctx, &resp.Attr)
}

func (n *WALNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f, err := os.OpenFile(n.db.WALPath(), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return newWALHandle(n, f), nil
}

func (n *WALNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f, err := os.Open(n.db.WALPath())
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

// lock tries to acquire a lock on a byte range of the node.
// If a conflicting lock is already held, returns syscall.EAGAIN.
func (n *WALNode) lock(ctx context.Context, req *fuse.LockRequest) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	// Parse lock range and ensure we are only performing one lock at a time.
	lockTypes := litefs.ParseWALLockRange(req.Lock.Start, req.Lock.End)
	if len(lockTypes) == 0 {
		return fmt.Errorf("no locks")
	} else if len(lockTypes) > 1 {
		return fmt.Errorf("cannot acquire multiple WAL locks at once")
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
		panic("fuse.WALNode.lock(): invalid POSIX lock type")
	}
}

// Unlock releases the lock on a byte range of the node. Locks can
// be released also implicitly, see HandleFlockLocker and HandlePOSIXLocker.
func (n *WALNode) unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, lockType := range litefs.ParseWALLockRange(req.Lock.Start, req.Lock.End) {
		guard := n.guardSet(req.LockOwner).Guard(lockType)
		guard.Unlock()
	}
	return nil
}

// QueryLock returns the current state of locks held for the byte range of the node.
func (n *WALNode) queryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	n.mu.Lock()
	defer n.mu.Unlock()

	for _, lockType := range litefs.ParseWALLockRange(req.Lock.Start, req.Lock.End) {
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
func (n *WALNode) canLock(owner fuse.LockOwner, typ fuse.LockType, lockType litefs.WALLockType) bool {
	guard := n.guardSet(owner).Guard(lockType)

	switch typ {
	case fuse.LockUnlock:
		return true
	case fuse.LockRead:
		return guard.CanRLock()
	case fuse.LockWrite:
		return guard.CanLock()
	default:
		panic("fuse.WALNode.canLock(): invalid POSIX lock type")
	}
}

func (n *WALNode) guardSet(owner fuse.LockOwner) *litefs.WALGuardSet {
	gs := n.guardSets[owner]
	if gs == nil {
		gs = n.db.WALGuardSet()
		n.guardSets[owner] = gs
	}
	return gs
}

// flush handles a handle flush request.
func (n *WALNode) flush(ctx context.Context, req *fuse.FlushRequest) error {
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

func (n *WALNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *WALNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *WALNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *WALNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *WALNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *WALNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}

var _ fs.Handle = (*WALHandle)(nil)
var _ fs.HandleReader = (*WALHandle)(nil)
var _ fs.HandleWriter = (*WALHandle)(nil)
var _ fs.HandlePOSIXLocker = (*WALHandle)(nil)

// WALHandle represents a file handle to a SQLite WAL file.
type WALHandle struct {
	node *WALNode
	file *os.File
}

func newWALHandle(node *WALNode, file *os.File) *WALHandle {
	return &WALHandle{node: node, file: file}
}

func (h *WALHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.file.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = buf[:n]
	return err
}

func (h *WALHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// TODO(wal): Generate SQLITE_READONLY for WAL.
	if err := h.node.db.WriteWAL(h.file, req.Data, req.Offset); err != nil {
		log.Printf("fuse: write(): wal error: %s", err)
		return ToError(err)
	}
	resp.Size = len(req.Data)
	return nil
}

func (h *WALHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return h.node.flush(ctx, req)
}

func (h *WALHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.file.Close()
}

func (h *WALHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	return h.node.lock(ctx, req)
}

func (h *WALHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	return fuse.Errno(syscall.ENOSYS)
}

func (h *WALHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	return h.node.unlock(ctx, req)
}

func (h *WALHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	return h.node.queryLock(ctx, req, resp)
}
