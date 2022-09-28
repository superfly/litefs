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
var _ fs.NodePoller = (*JournalNode)(nil)
var _ fs.Handle = (*JournalNode)(nil)
var _ fs.HandleReader = (*JournalNode)(nil)
var _ fs.HandleWriter = (*JournalNode)(nil)

// JournalNode represents a SQLite rollback journal file.
type JournalNode struct {
	fsys *FileSystem
	db   *litefs.DB
	file *os.File
}

func newJournalNode(fsys *FileSystem, db *litefs.DB, file *os.File) *JournalNode {
	return &JournalNode{fsys: fsys, db: db, file: file}
}

func (n *JournalNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	fi, err := n.file.Stat()
	if os.IsNotExist(err) {
		return fuse.ENOENT
	} else if err != nil {
		return err
	}

	attr.Mode = 0666
	attr.Size = uint64(fi.Size())
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
}

func (n *JournalNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}

func (n *JournalNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	nn, err := n.file.ReadAt(resp.Data, req.Offset)
	if nn != len(resp.Data) {
		return io.ErrShortBuffer
	}
	return err
}

func (n *JournalNode) Write(ctx context.Context, req *fuse.WriteRequest, resp *fuse.WriteResponse) error {
	if err := n.db.WriteJournal(n.file, req.Data, req.Offset); err != nil {
		log.Printf("fuse: write(): journal error: %s", err)
		return err
	}
	resp.Size = len(req.Data)
	return nil
}

func (n *JournalNode) Flush(ctx context.Context, req *fuse.FlushRequest) error { return nil }

func (n *JournalNode) Release(ctx context.Context, req *fuse.ReleaseRequest) error { return nil }

// Fsync performs an fsync() on the underlying file.
func (n *JournalNode) Fsync(ctx context.Context, req *fuse.FsyncRequest) error {
	return n.file.Sync()
}

func (n *JournalNode) Setattr(ctx context.Context, req *fuse.SetattrRequest, resp *fuse.SetattrResponse) error {
	// Only allow size updates.
	if req.Valid.Size() {
		if err := n.file.Truncate(int64(req.Size)); err != nil {
			return err
		}
	}

	return n.Attr(ctx, &resp.Attr)
}

func (n *JournalNode) Forget() {
	_ = n.file.Close()
	n.fsys.root.ForgetNode(n)
}

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

func (n *JournalNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}
