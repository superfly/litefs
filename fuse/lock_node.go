package fuse

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/superfly/litefs"
)

var _ fs.Node = (*LockNode)(nil)
var _ fs.NodeOpener = (*LockNode)(nil)
var _ fs.NodeForgetter = (*LockNode)(nil)
var _ fs.NodeListxattrer = (*LockNode)(nil)
var _ fs.NodeGetxattrer = (*LockNode)(nil)
var _ fs.NodeSetxattrer = (*LockNode)(nil)
var _ fs.NodeRemovexattrer = (*LockNode)(nil)
var _ fs.NodePoller = (*LockNode)(nil)

// LockNode represents a file used for non-standard SQLite locks. A separate
// file is needed because SQLite does not use OFD locks so cloing a handle will
// also release all locks.
type LockNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newLockNode(fsys *FileSystem, db *litefs.DB) *LockNode {
	return &LockNode{
		fsys: fsys,
		db:   db,
	}
}

func (n *LockNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = 0444
	attr.Size = 0
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	return nil
}

func (n *LockNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return newLockHandle(n), nil
}

func (n *LockNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *LockNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LockNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LockNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LockNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LockNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}

var _ fs.Handle = (*LockHandle)(nil)
var _ fs.HandleReader = (*LockHandle)(nil)
var _ fs.HandleWriter = (*LockHandle)(nil)
var _ fs.HandlePOSIXLocker = (*LockHandle)(nil)

// LockHandle represents a file handle to a SQLite database file.
type LockHandle struct {
	node *LockNode

	haltLockID     int64
	haltLock       *litefs.HaltLock
	haltLockMu     sync.Mutex
	haltLockCancel atomic.Value
}

func newLockHandle(node *LockNode) *LockHandle {
	h := &LockHandle{
		node:       node,
		haltLockID: rand.Int63(),
	}
	h.haltLockCancel.Store(context.CancelFunc(func() {}))
	return h
}

func (h *LockHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	resp.Data = resp.Data[:0]
	return nil
}

func (h *LockHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	log.Printf("fuse write error: cannot write to lock file")
	return syscall.EIO
}

func (h *LockHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	return h.unlockHalt(ctx)
}

func (h *LockHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return nil
}

func (h *LockHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	return syscall.ENOSYS
}

func (h *LockHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) (err error) {
	if req.Lock.Start != req.Lock.End {
		log.Printf("fuse lock error: only one lock can be acquired on the lock file at a time (%d..%d)", req.Lock.Start, req.Lock.End)
		return syscall.EINVAL
	}

	switch req.Lock.Start {
	case uint64(litefs.LockTypeHalt):
		return h.lockWaitHalt(ctx, req)
	default:
		log.Printf("fuse lock error: invalid lock file byte: %d", req.Lock.Start)
		return syscall.EINVAL
	}
}

func (h *LockHandle) lockWaitHalt(ctx context.Context, req *fuse.LockWaitRequest) (err error) {
	// Return an error this handle is already waiting for a halt lock.
	if !h.haltLockMu.TryLock() {
		log.Printf("lock wait error: handle is already waiting for halt lock")
		return syscall.ENOLCK
	}
	defer h.haltLockMu.Unlock()

	// Return an error if this handle is already holding a halt lock.
	if h.haltLock != nil {
		log.Printf("lock wait error: handle already acquired halt lock")
		return syscall.ENOLCK
	}

	// Ensure request is cancelable in case this handle is closed while we're waiting.
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	h.haltLockCancel.Store(cancel)

	switch typ := req.Lock.Type; typ {
	case fuse.LockWrite:
		// Attempt to acquire the remote lock. Return EAGAIN if we timeout and
		// return no error if this node is already the primary.
		h.haltLock, err = h.node.db.AcquireRemoteHaltLock(ctx, h.haltLockID)
		if errors.Is(err, context.Canceled) {
			if err := ctx.Err(); err != nil {
				return syscall.EINTR
			}
			return syscall.EAGAIN
		} else if err != nil && err != litefs.ErrNoHaltPrimary {
			return err
		}
		return nil

	case fuse.LockRead:
		return syscall.ENOSYS

	case fuse.LockUnlock: // Handled via Unlock() method
		return nil

	default:
		panic("fuse.lockWait(): invalid POSIX lock type")
	}
}

func (h *LockHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	if req.Lock.Start != req.Lock.End {
		log.Printf("fuse unlock error: only one lock can be released on the lock file at a time (%d..%d)", req.Lock.Start, req.Lock.End)
		return syscall.EINVAL
	}

	switch req.Lock.Start {
	case uint64(litefs.LockTypeHalt):
		return h.unlockHalt(ctx)
	default:
		log.Printf("fuse unlock error: invalid lock file byte: %d", req.Lock.Start)
		return syscall.EINVAL
	}
}

func (h *LockHandle) unlockHalt(ctx context.Context) error {
	if cancel := h.haltLockCancel.Load().(context.CancelFunc); cancel != nil {
		cancel()
	}

	h.haltLockMu.Lock()
	defer h.haltLockMu.Unlock()

	if h.haltLock == nil {
		return nil
	}

	err := h.node.db.ReleaseRemoteHaltLock(ctx, h.haltLockID)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return syscall.EINTR
	}
	h.haltLock = nil
	return err
}

func (h *LockHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	if req.Lock.Start != req.Lock.End {
		log.Printf("fuse query lock error: only one lock can be queried on the lock file at a time (%d..%d)", req.Lock.Start, req.Lock.End)
		return syscall.EINVAL
	}

	switch req.Lock.Start {
	case uint64(litefs.LockTypeHalt):
		if h.node.db.HasRemoteHaltLock() {
			resp.Lock = fuse.FileLock{
				Start: req.Lock.Start,
				End:   req.Lock.End,
				Type:  fuse.LockWrite,
				PID:   -1,
			}
		}
		return nil
	default:
		log.Printf("fuse query lock error: invalid lock file byte: %d", req.Lock.Start)
		return syscall.EINVAL
	}
}
