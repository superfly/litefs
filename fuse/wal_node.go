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
}

func newWALNode(fsys *FileSystem, db *litefs.DB) *WALNode {
	return &WALNode{
		fsys: fsys,
		db:   db,
	}
}

func (n *WALNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := os.Stat(n.db.WALPath())
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

func (n *WALNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	if req.Valid.Size() {
		if err := n.db.TruncateWAL(ctx, int64(req.Size)); err != nil {
			return err
		}
	}
	return n.Attr(ctx, &resp.Attr)
}

func (n *WALNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	resp.Flags |= fuse.OpenKeepCache

	f, err := n.db.OpenWAL(ctx)
	if err != nil {
		return nil, err
	}
	return newWALHandle(n, f), nil
}

func (n *WALNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return n.db.SyncWAL(ctx)
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

// WALHandle represents a file handle to a SQLite WAL file.
type WALHandle struct {
	node *WALNode
	file *os.File
}

func newWALHandle(node *WALNode, file *os.File) *WALHandle {
	return &WALHandle{node: node, file: file}
}

func (h *WALHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	n, err := h.node.db.ReadWALAt(ctx, h.file, resp.Data[:req.Size], req.Offset, uint64(req.LockOwner))
	if err == io.EOF {
		err = nil
	}
	resp.Data = resp.Data[:n]
	return err
}

func (h *WALHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	// TODO(wal): Generate SQLITE_READONLY for WAL.
	if err := h.node.db.WriteWALAt(ctx, h.file, req.Data, req.Offset, uint64(req.LockOwner)); err != nil {
		log.Printf("fuse: write(): wal error: %s", err)
		return ToError(err)
	}
	resp.Size = len(req.Data)
	return nil
}

func (h *WALHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	return h.node.db.CloseWAL(ctx, h.file, uint64(req.LockOwner))
}
