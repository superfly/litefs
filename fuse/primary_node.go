package fuse

import (
	"context"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

// PrimaryFilename is the name of the file that holds the current primary.
const PrimaryFilename = ".primary"

var _ fs.Node = (*PrimaryNode)(nil)
var _ fs.NodeForgetter = (*PrimaryNode)(nil)
var _ fs.NodeListxattrer = (*PrimaryNode)(nil)
var _ fs.NodeGetxattrer = (*PrimaryNode)(nil)
var _ fs.NodeSetxattrer = (*PrimaryNode)(nil)
var _ fs.NodeRemovexattrer = (*PrimaryNode)(nil)
var _ fs.NodePoller = (*PrimaryNode)(nil)
var _ fs.HandleReadAller = (*PrimaryNode)(nil)

// PrimaryNode represents a file for returning the current primary node.
type PrimaryNode struct {
	fsys *FileSystem
}

func newPrimaryNode(fsys *FileSystem) *PrimaryNode {
	return &PrimaryNode{fsys: fsys}
}

func (n *PrimaryNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	_, info := n.fsys.store.PrimaryInfo()
	if info == nil {
		return fuse.Errno(syscall.ENOENT)
	}

	attr.Mode = 0444
	attr.Size = uint64(len(info.Hostname) + 1)
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	return nil
}

func (n *PrimaryNode) ReadAll(ctx context.Context) ([]byte, error) {
	_, info := n.fsys.store.PrimaryInfo()
	if info == nil {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	return []byte(info.Hostname + "\n"), nil
}

func (n *PrimaryNode) Forget() { n.fsys.root.ForgetNode(n) }

// ENOSYS is a special return code for xattr requests that will be treated as a permanent failure for any such
// requests in the future without being sent to the filesystem.
// Source: https://github.com/libfuse/libfuse/blob/0b6d97cf5938f6b4885e487c3bd7b02144b1ea56/include/fuse_lowlevel.h#L811

func (n *PrimaryNode) Listxattr(ctx context.Context, req *fuse.ListxattrRequest, resp *fuse.ListxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PrimaryNode) Getxattr(ctx context.Context, req *fuse.GetxattrRequest, resp *fuse.GetxattrResponse) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PrimaryNode) Setxattr(ctx context.Context, req *fuse.SetxattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PrimaryNode) Removexattr(ctx context.Context, req *fuse.RemovexattrRequest) error {
	return fuse.ToErrno(syscall.ENOSYS)
}

func (n *PrimaryNode) Poll(ctx context.Context, req *fuse.PollRequest, resp *fuse.PollResponse) error {
	return fuse.Errno(syscall.ENOSYS)
}
