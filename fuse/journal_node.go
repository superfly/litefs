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

var _ fs.Node = (*JournalNode)(nil)
var _ fs.NodeForgetter = (*JournalNode)(nil)
var _ fs.NodeListxattrer = (*JournalNode)(nil)
var _ fs.NodeGetxattrer = (*JournalNode)(nil)
var _ fs.NodeSetxattrer = (*JournalNode)(nil)
var _ fs.NodeRemovexattrer = (*JournalNode)(nil)

// JournalNode represents a SQLite rollback journal file.
type JournalNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newJournalNode(fsys *FileSystem, db *litefs.DB) *JournalNode {
	return &JournalNode{fsys: fsys, db: db}
}

func (n *JournalNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := os.Stat(n.db.JournalPath())
	if err != nil {
		return err
	}

	attr.Mode = 0666
	attr.Size = uint64(fi.Size())
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
}

func (n *JournalNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	f, err := os.OpenFile(n.db.JournalPath(), os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}
	return newJournalHandle(n, f), nil
}

// Fsync performs an fsync() on the underlying file.
func (n *JournalNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	f, err := os.Open(n.db.JournalPath())
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

func (n *JournalNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Only allow size updates.
	if req.Valid.Size() {
		if err := os.Truncate(n.db.JournalPath(), int64(req.Size)); err != nil {
			return err
		}
	}

	return n.Attr(ctx, &resp.Attr)
}

func (n *JournalNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *JournalNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *JournalNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *JournalNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *JournalNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

var _ fs.Handle = (*JournalHandle)(nil)
var _ fs.HandleReader = (*JournalHandle)(nil)
var _ fs.HandleWriter = (*JournalHandle)(nil)

// JournalHandle represents a file handle to a SQLite journal file.
type JournalHandle struct {
	node *JournalNode
	file *os.File
}

func newJournalHandle(node *JournalNode, file *os.File) *JournalHandle {
	return &JournalHandle{node: node, file: file}
}

func (h *JournalHandle) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	n, err := h.file.ReadAt(resp.Data, req.Offset)
	if n != len(resp.Data) {
		return io.ErrShortBuffer
	}
	return err
}

func (h *JournalHandle) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if err := h.node.db.WriteJournal(h.file, req.Data, req.Offset); err != nil {
		log.Printf("fuse: write(): journal error: %s", err)
		return err
	}
	resp.Size = len(req.Data)
	return nil
}

func (h *JournalHandle) Flush(ctx context.Context, req *fuse.FlushRequest) error {
	_ = h.file.Close()
	return nil
}

func (h *JournalHandle) Release(ctx context.Context, req *fuse.ReleaseRequest) error {
	_ = h.file.Close()
	return nil
}
