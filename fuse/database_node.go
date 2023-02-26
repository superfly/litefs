package fuse

import (
	"context"
	"errors"
	"io"
	"log"
	"math/rand"
	"os"
	"sync"
	"sync/atomic"
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
		return syscall.ENOENT
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
		if err := n.db.TruncateDatabase(ctx, int64(req.Size)); err != nil {
			return err
		}
	}
	return n.Attr(ctx, &resp.Attr)
}

func (n *DatabaseNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenKeepCache

	f, err := n.db.OpenDatabase(ctx)
	if err != nil {
		return nil, err
	}
	return newDatabaseHandle(n, f), nil
}

func (n *DatabaseNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return n.db.SyncDatabase(ctx)
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

	haltLockID     int64
	haltLock       *litefs.HaltLock
	haltLockMu     sync.Mutex
	haltLockCancel atomic.Value
}

func newDatabaseHandle(node *DatabaseNode, file *os.File) *DatabaseHandle {
	h := &DatabaseHandle{
		node:       node,
		file:       file,
		haltLockID: rand.Int63(),
	}
	h.haltLockCancel.Store(context.CancelFunc(func() {}))
	return h
}

func (h *DatabaseHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	n, err := h.node.db.ReadDatabaseAt(ctx, h.file, resp.Data[:req.Size], req.Offset, uint64(req.LockOwner))
	if err == io.EOF {
		err = nil
	}
	resp.Data = resp.Data[:n]
	return err
}

func (h *DatabaseHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if err := h.node.db.WriteDatabaseAt(ctx, h.file, req.Data, req.Offset, uint64(req.LockOwner)); err != nil {
		log.Printf("fuse: write(): database error: %s", err)
		return err
	}
	resp.Size = len(req.Data)
	return nil
}

func (h *DatabaseHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	err := h.unlockHaltLock(ctx)
	h.node.db.UnlockDatabase(ctx, uint64(req.LockOwner))
	return err
}

func (h *DatabaseHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.node.db.CloseDatabase(ctx, h.file, uint64(req.LockOwner))
}

func (h *DatabaseHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	lockTypes := litefs.ParseDatabaseLockRange(req.Lock.Start, req.Lock.End)
	return lock(ctx, req, h.node.db, lockTypes)
}

func (h *DatabaseHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) (err error) {
	// We only support the HALT lock with F_SETLKW.
	if req.Lock.Start != uint64(litefs.LockTypeHalt) || req.Lock.End != uint64(litefs.LockTypeHalt) {
		return syscall.EINVAL
	}

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
	case fuse.LockUnlock: // Handled via Unlock() method
		return nil

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

	default:
		panic("fuse.lockWait(): invalid POSIX lock type")
	}
}

func (h *DatabaseHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	// Release the remote HALT lock, if it is acquired. If not, this is a no-op.
	if req.Lock.Start == uint64(litefs.LockTypeHalt) && req.Lock.End == uint64(litefs.LockTypeHalt) {
		return h.unlockHaltLock(ctx)
	}

	// Otherwise process other locks normally.
	lockTypes := litefs.ParseDatabaseLockRange(req.Lock.Start, req.Lock.End)
	return h.node.db.Unlock(ctx, uint64(req.LockOwner), lockTypes)
}

func (h *DatabaseHandle) unlockHaltLock(ctx context.Context) error {
	if cancel := h.haltLockCancel.Load().(context.CancelFunc); cancel != nil {
		cancel()
	}

	h.haltLockMu.Lock()
	defer h.haltLockMu.Unlock()

	if h.haltLock == nil {
		return nil
	}

	err := h.node.db.ReleaseRemoteHaltLock(ctx, h.haltLock.ID)
	if errors.Is(err, context.Canceled) && ctx.Err() != nil {
		return syscall.EINTR
	}
	h.haltLock = nil
	return err
}

func (h *DatabaseHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	lockTypes := litefs.ParseDatabaseLockRange(req.Lock.Start, req.Lock.End)
	queryLock(ctx, req, resp, h.node.db, lockTypes)
	return nil
}

func lock(ctx context.Context, req *fuse.LockRequest, db *litefs.DB, lockTypes []litefs.LockType) error {
	switch typ := req.Lock.Type; typ {
	case fuse.LockUnlock:
		return nil

	case fuse.LockWrite:
		if ok, err := db.TryLocks(ctx, uint64(req.LockOwner), lockTypes); err != nil {
			log.Printf("fuse lock error: %s", err)
			return err
		} else if !ok {
			return syscall.EAGAIN
		}
		return nil

	case fuse.LockRead:
		if !db.TryRLocks(ctx, uint64(req.LockOwner), lockTypes) {
			return syscall.EAGAIN
		}
		return nil

	default:
		panic("fuse.lock(): invalid POSIX lock type")
	}
}

func queryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse, db *litefs.DB, lockTypes []litefs.LockType) {
	switch req.Lock.Type {
	case fuse.LockRead:
		if !db.CanRLock(ctx, uint64(req.LockOwner), lockTypes) {
			resp.Lock = fuse.FileLock{
				Start: req.Lock.Start,
				End:   req.Lock.End,
				Type:  fuse.LockWrite,
				PID:   -1,
			}
		}
	case fuse.LockWrite:
		if canLock, mutexState := db.CanLock(ctx, uint64(req.LockOwner), lockTypes); !canLock {
			resp.Lock = fuse.FileLock{
				Start: req.Lock.Start,
				End:   req.Lock.End,
				Type:  fuse.LockRead,
				PID:   -1,
			}
			if mutexState == litefs.RWMutexStateExclusive {
				resp.Lock.Type = fuse.LockWrite
			}
		}
	}
}
