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

// DatabaseNode represents a SQLite database file.
type DatabaseNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newDatabaseNode(fsys *FileSystem, db *litefs.DB) *DatabaseNode {
	return &DatabaseNode{fsys: fsys, db: db}
}

func (n *DatabaseNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := os.Stat(n.db.DatabasePath())
	if err != nil {
		return err
	}

	attr.Mode = 0666
	attr.Size = uint64(fi.Size())
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
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

var _ fs.Handle = (*DatabaseHandle)(nil)
var _ fs.HandleReader = (*DatabaseHandle)(nil)
var _ fs.HandleWriter = (*DatabaseHandle)(nil)
var _ fs.HandlePOSIXLocker = (*DatabaseHandle)(nil)

// DatabaseHandle represents a file handle to a SQLite database file.
type DatabaseHandle struct {
	node *DatabaseNode
	file *os.File

	// SQLite locks held
	pendingGuard  *litefs.RWMutexGuard
	sharedGuard   *litefs.RWMutexGuard
	reservedGuard *litefs.RWMutexGuard
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
	// TODO: Obtain handle lock.

	for _, mu := range []**litefs.RWMutexGuard{&h.pendingGuard, &h.sharedGuard, &h.reservedGuard} {
		if *mu != nil {
			(*mu).Unlock()
			*mu = nil
		}
	}

	return nil
}

func (h *DatabaseHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.file.Close()
}

// Lock tries to acquire a lock on a byte range of the node.
// If a conflicting lock is already held, returns syscall.EAGAIN.
func (h *DatabaseHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	// Parse lock range and ensure we are only performing one lock at a time.
	lockTypes := litefs.ParseLockRange(req.Lock.Start, req.Lock.End)
	if len(lockTypes) == 0 {
		return fmt.Errorf("no locks")
	} else if len(lockTypes) > 1 {
		return fmt.Errorf("cannot acquire multiple locks at once")
	}
	lockType := lockTypes[0]

	// TODO: Hold file handle lock for rest of function.
	mu, guard, err := h.mutexAndGuardRefByLockType(lockType)
	if err != nil {
		return err
	}

	if *guard != nil {
		switch typ := req.Lock.Type; typ {
		case fuse.LockRead:
			(*guard).RLock()
			return nil
		case fuse.LockWrite:
			if !(*guard).TryLock() {
				return fuse.Errno(syscall.EAGAIN)
			}
			return nil
		default:
			panic(fmt.Sprintf("invalid posix lock type: %d", typ))
		}
	}

	switch typ := req.Lock.Type; typ {
	case fuse.LockRead:
		if *guard = mu.TryRLock(); *guard == nil {
			return fuse.Errno(syscall.EAGAIN)
		}
		return nil
	case fuse.LockWrite:
		if *guard = mu.TryLock(); *guard == nil {
			return fuse.Errno(syscall.EAGAIN)
		}
		return nil
	default:
		panic(fmt.Sprintf("invalid posix lock type: %d", typ))
	}
}

// LockWait is not implemented as SQLite does not use setlkw.
func (h *DatabaseHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	return fuse.Errno(syscall.ENOSYS)
}

// Unlock releases the lock on a byte range of the node. Locks can
// be released also implicitly, see HandleFlockLocker and
// HandlePOSIXLocker.
func (h *DatabaseHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	lockTypes := litefs.ParseLockRange(req.Lock.Start, req.Lock.End)

	for _, lockType := range lockTypes {
		_, guard, err := h.mutexAndGuardRefByLockType(lockType)
		if err != nil {
			return err
		} else if *guard == nil {
			continue // no lock acquired, skip
		}

		// Unlock and drop reference to the guard.
		(*guard).Unlock()
		*guard = nil
	}
	return nil
}

// QueryLock returns the current state of locks held for the byte range of the node.
func (h *DatabaseHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	lockTypes := litefs.ParseLockRange(req.Lock.Start, req.Lock.End)

	for _, lockType := range lockTypes {
		ok, err := h.canLock(req.Lock.Type, lockType)
		if err != nil {
			return err
		}

		if !ok {
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
func (h *DatabaseHandle) canLock(typ fuse.LockType, lockType litefs.LockType) (bool, error) {
	// TODO: Hold file handle lock

	mu, guard, err := h.mutexAndGuardRefByLockType(lockType)
	if err != nil {
		return false, err
	}

	if *guard != nil {
		switch typ {
		case syscall.F_UNLCK:
			return true, nil
		case syscall.F_RDLCK:
			return true, nil
		case syscall.F_WRLCK:
			return (*guard).CanLock(), nil
		default:
			panic(fmt.Sprintf("invalid posix lock type: %d", typ))
		}
	}

	switch typ {
	case syscall.F_UNLCK:
		return true, nil
	case syscall.F_RDLCK:
		return mu.CanRLock(), nil
	case syscall.F_WRLCK:
		return mu.CanLock(), nil
	default:
		panic(fmt.Sprintf("invalid posix lock type: %d", typ))
	}
}

// mutexAndGuardRefByLockType returns the mutex and, if held, a guard for that mutex.
func (h *DatabaseHandle) mutexAndGuardRefByLockType(lockType litefs.LockType) (mu *litefs.RWMutex, guardRef **litefs.RWMutexGuard, err error) {
	switch lockType {
	case litefs.LockTypePending:
		return h.node.db.PendingLock(), &h.pendingGuard, nil
	case litefs.LockTypeReserved:
		return h.node.db.ReservedLock(), &h.reservedGuard, nil
	case litefs.LockTypeShared:
		return h.node.db.SharedLock(), &h.sharedGuard, nil
	default:
		return nil, nil, fmt.Errorf("invalid lock type: %d", lockType)
	}
}
