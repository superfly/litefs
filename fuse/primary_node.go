package fuse

import (
	"context"
	"syscall"

	"bazil.org/fuse"
	"bazil.org/fuse/fs"
)

var _ fs.Node = (*PrimaryNode)(nil)

// PrimaryNode represents a file for returning the current primary node.
type PrimaryNode struct {
	fsys *FileSystem
}

func newPrimaryNode(fsys *FileSystem) *PrimaryNode {
	return &PrimaryNode{fsys: fsys}
}

func (n *PrimaryNode) Attr(ctx context.Context, attr *fuse.Attr) error {
	attr.Mode = 0444
	attr.Uid = uint32(n.fsys.Uid)
	attr.Gid = uint32(n.fsys.Gid)
	return nil
}

func (n *PrimaryNode) ReadAll(ctx context.Context) ([]byte, error) {
	primaryURL := n.fsys.store.PrimaryURL()
	if primaryURL == "" {
		return nil, fuse.Errno(syscall.ENOENT)
	}
	return []byte(primaryURL + "\n"), nil
}
