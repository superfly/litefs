package fuse

import (
	"context"
	"strconv"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

const LagFilename = ".lag"

var _ fs.Node = (*LagNode)(nil)
var _ fs.NodeForgetter = (*LagNode)(nil)
var _ fs.HandleReadAller = (*LagNode)(nil)
var _ fs.NodeListxattrer = (*PosNode)(nil)
var _ fs.NodeGetxattrer = (*PosNode)(nil)
var _ fs.NodeSetxattrer = (*PosNode)(nil)
var _ fs.NodeRemovexattrer = (*PosNode)(nil)
var _ fs.NodePoller = (*PosNode)(nil)

type LagNode struct {
	fsys *FileSystem
}

func newLagNode(fsys *FileSystem) *LagNode {
	return &LagNode{fsys: fsys}
}

func (n *LagNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = 0o444
	attr.Size = uint64(len(n.content()))
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
}

func (n *LagNode) Forget() { n.fsys.root.ForgetNode(n) }

func (n *LagNode) ReadAll(ctx context.Context) ([]byte, error) {
	return n.content(), nil
}

func (n *LagNode) content() []byte {
	if ts := n.fsys.store.PrimaryTimestamp(); ts > 0 {
		return []byte(strconv.FormatInt(ts, 10))
	}
	return []byte("-1")
}

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *LagNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LagNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LagNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LagNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *LagNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}
