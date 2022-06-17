package litefs

import (
	"context"
	"log"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

// Node represents an inode in the FUSE filesystem.
type Node struct {
	*fs.LoopbackNode
	root     *Root
	name     string
	fileType FileType
	db       *DB
}

// Name returns the node's file path from the root.
func (n *Node) Name() string {
	return n.name
}

// Path returns the absolute path of the node (including the root).
func (n *Node) Path() string {
	return filepath.Join(n.root.Path, n.name)
}

func (n *Node) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	inode, fh, fuseFlags, errno = n.LoopbackNode.Create(ctx, name, flags, mode, out)
	if errno != 0 {
		return inode, fh, fuseFlags, errno
	}

	node := inode.Operations().(*Node)
	return inode, &FileHandle{fsFileHandle: fh.(fsFileHandle), node: node}, fuseFlags, errno
}

func (n *Node) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	fh, fuseFlags, errno = n.LoopbackNode.Open(ctx, flags)
	if errno != 0 {
		return fh, fuseFlags, errno
	}
	return &FileHandle{fsFileHandle: fh.(fsFileHandle), node: n}, fuseFlags, errno
}

func (n *Node) Unlink(ctx context.Context, name string) syscall.Errno {
	// Copy pages out to LTX file if we are removing a rollback journal.
	if strings.HasSuffix(name, "-journal") {
		// TODO: Check that RESERVED lock is currently held.

		if err := n.db.WriteJournalLTX(ctx); err != nil {
			log.Printf("error writing journal ltx file: %s", err)
			return syscall.EIO
		}
	}

	errno := n.LoopbackNode.Unlink(ctx, name)
	log.Printf("unlink(%q, %q) => <%d>", n.Name(), name, errno)
	return errno
}

func (n *Node) Getlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	if f, ok := f.(fs.FileGetlker); ok {
		return f.Getlk(ctx, owner, lk, flags, out)
	}
	return syscall.Errno(fuse.ENOTSUP)
}

func (n *Node) Setlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	if f, ok := f.(fs.FileSetlker); ok {
		return f.Setlk(ctx, owner, lk, flags)
	}
	return syscall.Errno(fuse.ENOTSUP)
}

func (n *Node) Setlkw(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	if f, ok := f.(fs.FileSetlkwer); ok {
		return f.Setlkw(ctx, owner, lk, flags)
	}
	return syscall.Errno(fuse.ENOTSUP)
}
