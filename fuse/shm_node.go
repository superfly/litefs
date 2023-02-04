package fuse

import (
	"context"
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
		return syscall.ENOENT
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
		if err := n.db.TruncateSHM(ctx, int64(req.Size)); err != nil {
			return err
		}
	}
	return n.Attr(ctx, &resp.Attr)
}

func (n *SHMNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenKeepCache

	f, err := n.db.OpenSHM(ctx)
	if err != nil {
		return nil, err
	}
	return newSHMHandle(n, f), nil
}

func (n *SHMNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return n.db.SyncSHM(ctx)
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
	n, err := h.node.db.ReadSHMAt(ctx, h.file, resp.Data[:req.Size], req.Offset, uint64(req.LockOwner))
	if err == io.EOF {
		err = nil
	}
	resp.Data = resp.Data[:n]
	return err
}

func (h *SHMHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	n, err := h.node.db.WriteSHMAt(ctx, h.file, req.Data, req.Offset, uint64(req.LockOwner))
	resp.Size = n
	if err != nil {
		log.Printf("fuse: write(): shm error: %s", err)
		return err
	}
	return nil
}

func (h *SHMHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	h.node.db.UnlockSHM(ctx, uint64(req.LockOwner))
	return nil
}

func (h *SHMHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.node.db.CloseSHM(ctx, h.file, uint64(req.LockOwner))
}

func (h *SHMHandle) Lock(ctx context.Context, req *fuse.LockRequest) error {
	lockTypes := litefs.ParseSHMLockRange(req.Lock.Start, req.Lock.End)
	return lock(ctx, req, h.node.db, lockTypes)
}

func (h *SHMHandle) LockWait(ctx context.Context, req *fuse.LockWaitRequest) error {
	return fuse.Errno(syscall.ENOSYS)
}

func (h *SHMHandle) Unlock(ctx context.Context, req *fuse.UnlockRequest) (err error) {
	lockTypes := litefs.ParseSHMLockRange(req.Lock.Start, req.Lock.End)
	h.node.db.Unlock(ctx, uint64(req.LockOwner), lockTypes)
	return nil
}

func (h *SHMHandle) QueryLock(ctx context.Context, req *fuse.QueryLockRequest, resp *fuse.QueryLockResponse) error {
	lockTypes := litefs.ParseSHMLockRange(req.Lock.Start, req.Lock.End)
	queryLock(ctx, req, resp, h.node.db, lockTypes)
	return nil
}
