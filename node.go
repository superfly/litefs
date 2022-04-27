package litefs

import (
	"context"
	"path/filepath"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Node represents an inode in the FUSE filesystem.
type Node struct {
	*fs.LoopbackNode
	root *Root
	name string
}

func (n *Node) fullName() string {
	return filepath.Join(n.root.Path, n.name)
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	inode, fh, fuseFlags, errno = n.LoopbackNode.Create(ctx, name, flags, mode, out)
	if errno != 0 {
		return inode, fh, fuseFlags, errno
	}
	return inode, &FileHandle{fsFileHandle: fh.(fsFileHandle), node: n}, fuseFlags, errno
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fh, fuseFlags, errno = n.LoopbackNode.Open(ctx, flags)
	if errno != 0 {
		return fh, fuseFlags, errno
	}
	return &FileHandle{fsFileHandle: fh.(fsFileHandle), node: n}, fuseFlags, errno
}
