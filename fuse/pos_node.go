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

// PosFileSize is the size, in bytes, of the "-pos" file.
const PosFileSize = 34

var (
	_ fs.Node              = (*PosNode)(nil)
	_ fs.NodeOpener        = (*PosNode)(nil)
	_ fs.NodeForgetter     = (*PosNode)(nil)
	_ fs.NodeListxattrer   = (*PosNode)(nil)
	_ fs.NodeGetxattrer    = (*PosNode)(nil)
	_ fs.NodeSetxattrer    = (*PosNode)(nil)
	_ fs.NodeRemovexattrer = (*PosNode)(nil)
	_ fs.NodePoller        = (*PosNode)(nil)
	_ fs.HandleReader      = (*PosNode)(nil)
)

// PosNode represents a file that returns the current position of the database.
type PosNode struct {
	fsys *FileSystem
	db   *litefs.DB
}

func newPosNode(fsys *FileSystem, db *litefs.DB) *PosNode {
	return &PosNode{fsys: fsys, db: db}
}

func (n *PosNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = 0o666
	attr.Size = uint64(PosFileSize)
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	attr.Mtime = n.db.Timestamp()
	return nil
}

func (n *PosNode) Open(ctx context.Context, req *fuse.OpenRequest, resp *fuse.OpenResponse) (fs.Handle, error) {
	return n, nil
}

func (n *PosNode) Read(ctx context.Context, req *fuse.ReadRequest, resp *fuse.ReadResponse) error {
	pos := n.db.Pos()

	data := fmt.Sprintf("%s/%s\n", pos.TXID, pos.PostApplyChecksum)
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

func (n *PosNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *PosNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PosNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PosNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PosNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PosNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}
