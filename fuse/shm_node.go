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

var _ fs.Node = (*SHMNode)(nil)
var _ fs.NodeOpener = (*SHMNode)(nil)
var _ fs.NodeFsyncer = (*SHMNode)(nil)
var _ fs.NodeForgetter = (*SHMNode)(nil)
var _ fs.NodeListxattrer = (*SHMNode)(nil)
var _ fs.NodeGetxattrer = (*SHMNode)(nil)
var _ fs.NodeSetxattrer = (*SHMNode)(nil)
var _ fs.NodeRemovexattrer = (*SHMNode)(nil)
var _ fs.NodePoller = (*SHMNode)(nil)

// SHMNode represents a SQLite database file.
type SHMNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newSHMNode(fsys *FileSystem, db *litefs.DB) *SHMNode {
	return &SHMNode{
		fsys: fsys,
		db:   db,
	}
}

func (n *SHMNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := os.Stat(n.db.SHMPath())
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

func (n *SHMNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		if err := os.Truncate(n.db.SHMPath(), int64(req.Size)); err != nil {
			return err
		}
	}
	return n.Attr(ctx, &resp.Attr)
}

func (n *SHMNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f, err := os.OpenFile(n.db.SHMPath(), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return newSHMHandle(n, f), nil
}

func (n *SHMNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f, err := os.Open(n.db.SHMPath())
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

func (n *SHMNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *SHMNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *SHMNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *SHMNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *SHMNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *SHMNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}

var _ fs.Handle = (*SHMHandle)(nil)
var _ fs.HandleReader = (*SHMHandle)(nil)
var _ fs.HandleWriter = (*SHMHandle)(nil)
var _ fs.HandlePOSIXLocker = (*SHMHandle)(nil)

// SHMHandle represents a file handle to a SQLite database file.
type SHMHandle struct {
	node *SHMNode
	file *os.File
}

func newSHMHandle(node *SHMNode, file *os.File) *SHMHandle {
	return &SHMHandle{node: node, file: file}
}

func (h *SHMHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	buf := make([]byte, req.Size)
	n, err := h.file.ReadAt(buf, req.Offset)
	if err == io.EOF {
		err = nil
	}
	resp.Data = buf[:n]
	return err
}

func (h *SHMHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := h.node.db.WriteSHM(h.file, req.Data, req.Offset)
	resp.Size = n
	if err != nil {
		log.Printf("fuse: write(): shm error: %s", err)
		return err
	}
	return nil
}

func (h *SHMHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	if gs := h.node.fsys.GuardSet(h.node.db, req.LockOwner); gs != nil {
		gs.UnlockSHM()
	}
	return nil
}

func (h *SHMHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.file.Close()
}

func (h *SHMHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	// Parse lock range and ensure we are only performing one lock at a time.
	lockTypes := litefs.ParseWALLockRange(req.Lock.Start, req.Lock.End)
	if len(lockTypes) == 0 {
		return fmt.Errorf("no wal locks")
	}

	for _, lockType := range lockTypes {
		guard := h.node.fsys.CreateGuardSetIfNotExists(h.node.db, req.LockOwner).Guard(lockType)

		switch typ := req.Lock.Type; typ {
		case fuse.LockRead:
			if !guard.TryRLock() {
				return syscall.EAGAIN
			}
		case fuse.LockWrite:
			if !guard.TryLock() {
				return syscall.EAGAIN
			}
		default:
			panic("fuse.SHMNode.lock(): invalid POSIX lock type")
		}
	}
	return nil
}

func (h *SHMHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	return fuse.Errno(syscall.ENOSYS)
}

func (h *SHMHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) error {
	for _, lockType := range litefs.ParseWALLockRange(req.Lock.Start, req.Lock.End) {
		if gs := h.node.fsys.GuardSet(h.node.db, req.LockOwner); gs != nil {
			gs.Guard(lockType).Unlock()
		}
	}
	return nil
}

func (h *SHMHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	for _, lockType := range litefs.ParseWALLockRange(req.Lock.Start, req.Lock.End) {
		canLock, blockingLockType := h.canLock(req.LockOwner, req.Lock.Type, lockType)
		if canLock {
			continue
		}

		resp.Lock = fuse.FileLock{
			Start: req.Lock.Start,
			End:   req.Lock.End,
			Type:  blockingLockType,
			PID:   -1,
		}
		return nil
	}
	return nil
}

// canLock returns true if the given lock can be acquired. If false,
func (h *SHMHandle) canLock(owner fuse.LockOwner, typ fuse.LockType, lockType litefs.LockType) (bool, fuse.LockType) {
	gs := h.node.fsys.CreateGuardSetIfNotExists(h.node.db, owner)
	guard := gs.Guard(lockType)

	switch typ {
	case fuse.LockUnlock:
		return true, fuse.LockUnlock
	case fuse.LockRead:
		if guard.CanRLock() {
			return true, fuse.LockUnlock
		}
		return false, fuse.LockWrite
	case fuse.LockWrite:
		if canLock, mutexState := guard.CanLock(); canLock {
			return true, fuse.LockUnlock
		} else if mutexState == litefs.RWMutexStateExclusive {
			return false, fuse.LockWrite
		}
		return false, fuse.LockRead
	default:
		panic("fuse.SHMHandle.canLock(): invalid POSIX lock type")
	}
}
