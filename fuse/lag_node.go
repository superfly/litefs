package fuse

import (
	"context"
	"fmt"
	"math"
	"syscall"
	"time"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

const LagFilename = ".lag"

// The lag file is constantly changing, but we need to be able to
// say how big it is. So, we add leading zeros and always include
// the sign to give it a fixed size.
//
//	  var (
//		  maxLagDigits = len(fmt.Sprintf("%d", math.MaxInt32))
//		  lagFmt       = fmt.Sprintf("%%+0%dd\n", maxLagDigits)
//		  lagSize      = len(fmt.Sprintf(lagFmt, 0))
//	  )
const (
	lagFmt  = "%+010d\n"
	lagSize = 11
)

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
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	attr.Valid = 0
	attr.Size = lagSize

	switch ts := n.fsys.store.PrimaryTimestamp(); ts {
	case 0: // we're the primary
		attr.Mtime = time.Now()
	case -1: // haven't finished initial replication
		attr.Mtime = time.UnixMilli(0)
	default:
		attr.Mtime = time.UnixMilli(ts)
	}

	return nil
}

func (n *LagNode) Forget() { n.fsys.root.ForgetNode(n) }

func (n *LagNode) ReadAll(ctx context.Context) ([]byte, error) {
	switch ts := n.fsys.store.PrimaryTimestamp(); ts {
	case 0:
		return []byte(fmt.Sprintf(lagFmt, 0)), nil
	case -1:
		return []byte(fmt.Sprintf(lagFmt, math.MaxInt32)), nil
	default:
		return []byte(fmt.Sprintf(lagFmt, time.Now().UnixMilli()-ts)), nil
	}
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
