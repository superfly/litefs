package litefs

import (
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
)

// Root represents the FUSE root.
type Root struct {
	*fs.LoopbackRoot
}

// NewRoot returns a new instance of Root.
func NewRoot(path string, dev uint64) *Root {
	root := &Root{
		LoopbackRoot: &fs.LoopbackRoot{
			Path: path,
			Dev:  dev,
		},
	}

	root.NewNode = root.newNode
	return root
}

// newNode provides an injection mechanism to add our own Node type.
func (r *Root) newNode(lpRoot *fs.LoopbackRoot, parent *fs.Inode, name string, st *syscall.Stat_t) fs.InodeEmbedder {
	return &Node{
		LoopbackNode: &fs.LoopbackNode{RootData: lpRoot},
		root:         r,
		name:         name,
	}
}
