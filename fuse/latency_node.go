package fuse

import (
	"context"
	"fmt"
	"io"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
	"github.com/superfly/litefs"
)

var _ fs.Node = (*LatencyNode)(nil)
var _ fs.NodeOpener = (*LatencyNode)(nil)
var _ fs.NodeForgetter = (*LatencyNode)(nil)
var _ fs.NodeListxattrer = (*LatencyNode)(nil)
var _ fs.NodeGetxattrer = (*LatencyNode)(nil)
var _ fs.NodeSetxattrer = (*LatencyNode)(nil)
var _ fs.NodeRemovexattrer = (*LatencyNode)(nil)
var _ fs.NodePoller = (*LatencyNode)(nil)

// LatencyNode represents a file that returns the current position of the database.
type LatencyNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newLatencyNode(fsys *FileSystem, db *litefs.DB) *LatencyNode {
	return &LatencyNode{fsys: fsys, db: db}
}

func (n *LatencyNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = 0666
	attr.Size = uint64(PosFileSize)
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	return nil
}

func (n *LatencyNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}

func (n *LatencyNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	latency := n.db.Latency()

	data := fmt.Sprintf("%d\n", latency)
	if req.Offset >= int64(len(data)) {
		return io.EOF
	}

	// Size buffer to offset/size.
	data = data[req.Offset:]
	if len(data) > req.Size {
		data = data[:req.Size]
	}
	resp.Data = []byte(data)

	return nil
}

func (n *LatencyNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *LatencyNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LatencyNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LatencyNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LatencyNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LatencyNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}
