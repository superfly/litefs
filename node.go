package litefs

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"golang.org/x/sys/unix"
)

var _ fs.NodeStatfser = (*RootNode)(nil)
var _ fs.NodeLookuper = (*RootNode)(nil)
var _ fs.NodeOpendirer = (*RootNode)(nil)
var _ fs.NodeReaddirer = (*RootNode)(nil)
var _ fs.NodeGetattrer = (*RootNode)(nil)
var _ fs.NodeCreater = (*RootNode)(nil)
var _ fs.NodeUnlinker = (*RootNode)(nil)

// RootNode represents the top-level mounted directory.
type RootNode struct {
	fs.Inode

	dev  uint64
	path string

	mu  sync.Mutex
	dbs map[string]*DB

	URL string
}

// NewRootNode returns a new instance of RootNode.
func NewRootNode(dev uint64, path string) *RootNode {
	return &RootNode{
		dev:  dev,
		path: path,
		dbs:  make(map[string]*DB),
	}
}

// DB returns a database by name.
func (n *RootNode) DB(name string) *DB {
	n.mu.Lock()
	defer n.mu.Unlock()

	db := n.dbs[name]
	if db == nil {
		db = NewDB(n, name)
		n.dbs[name] = db
	}
	return db
}

func (n *RootNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	var s syscall.Statfs_t
	if err := syscall.Statfs(n.path, &s); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return fs.OK
}

func (n *RootNode) Lookup(ctx context.Context, name string, out *fuse.EntryOut) (*fs.Inode, syscall.Errno) {
	var st syscall.Stat_t
	if err := syscall.Lstat(filepath.Join(n.path, name), &st); err != nil {
		return nil, fs.ToErrno(err)
	}

	out.Attr.FromStat(&st)
	return n.NewInode(ctx, NewFileNode(n, name, n.DB(TrimName(name))), n.idFromStat(&st)), 0
}

func (n *RootNode) Opendir(ctx context.Context) syscall.Errno {
	fd, err := syscall.Open(n.path, syscall.O_DIRECTORY, 0755)
	if err != nil {
		return fs.ToErrno(err)
	}
	syscall.Close(fd)
	return fs.OK
}

func (n *RootNode) Readdir(ctx context.Context) (fs.DirStream, syscall.Errno) {
	return NewDirStream(n.path)
}

func (n *RootNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f != nil {
		return f.(fs.FileGetattrer).Getattr(ctx, out)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(n.path, &st); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return fs.OK
}

func (n *RootNode) Create(ctx context.Context, name string, flags uint32, mode uint32, out *fuse.EntryOut) (inode *fs.Inode, fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// flags = flags &^ syscall.O_APPEND

	path := filepath.Join(n.path, name)

	fd, err := syscall.Open(path, int(flags)|os.O_CREATE, mode)
	if err != nil {
		return nil, nil, 0, fs.ToErrno(err)
	}

	// Preserve owner
	if os.Getuid() == 0 {
		if caller, ok := fuse.FromContext(ctx); ok {
			syscall.Lchown(path, int(caller.Uid), int(caller.Gid))
		}
	}

	var st syscall.Stat_t
	if err := syscall.Fstat(fd, &st); err != nil {
		syscall.Close(fd)
		return nil, nil, 0, fs.ToErrno(err)
	}

	ch := n.NewInode(ctx, NewFileNode(n, name, n.DB(TrimName(name))), n.idFromStat(&st))
	out.FromStat(&st)
	return ch, NewFileHandle(fd, n.DB(TrimName(name)), name), 0, 0
}

func (n *RootNode) Unlink(ctx context.Context, name string) syscall.Errno {
	return fs.ToErrno(syscall.Unlink(filepath.Join(n.path, name)))
}

func (n *RootNode) idFromStat(st *syscall.Stat_t) fs.StableAttr {
	swapped := (uint64(st.Dev) << 32) | (uint64(st.Dev) >> 32)
	swappedRootDev := (n.dev << 32) | (n.dev >> 32)
	return fs.StableAttr{
		Mode: uint32(st.Mode),
		Gen:  1,
		Ino:  (swapped ^ swappedRootDev) ^ st.Ino,
	}
}

var _ fs.NodeStatfser = (*FileNode)(nil)
var _ fs.NodeOpener = (*FileNode)(nil)
var _ fs.NodeGetattrer = (*FileNode)(nil)
var _ fs.NodeSetattrer = (*FileNode)(nil)
var _ fs.NodeGetlker = (*FileNode)(nil)
var _ fs.NodeSetlker = (*FileNode)(nil)
var _ fs.NodeSetlkwer = (*FileNode)(nil)

// FileNode represents the a file within the top-level directory.
type FileNode struct {
	fs.Inode

	root *RootNode
	name string
	db   *DB
}

func NewFileNode(root *RootNode, name string, db *DB) *FileNode {
	return &FileNode{
		root: root,
		name: name,
		db:   db,
	}
}

// path returns the path of the underlying file.
func (n *FileNode) path() string {
	return filepath.Join(n.root.path, n.name)
}

func (n *FileNode) Statfs(ctx context.Context, out *fuse.StatfsOut) syscall.Errno {
	var s syscall.Statfs_t
	if err := syscall.Statfs(n.path(), &s); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStatfsT(&s)
	return fs.OK
}

func (n *FileNode) Getattr(ctx context.Context, f fs.FileHandle, out *fuse.AttrOut) syscall.Errno {
	if f != nil {
		return f.(fs.FileGetattrer).Getattr(ctx, out)
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(n.path(), &st); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return fs.OK
}

func (n *FileNode) Getxattr(ctx context.Context, attr string, dest []byte) (uint32, syscall.Errno) {
	sz, err := unix.Lgetxattr(n.path(), attr, dest)
	return uint32(sz), fs.ToErrno(err)
}

func (n *FileNode) Setattr(ctx context.Context, f fs.FileHandle, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	path := n.path()

	fsa, ok := f.(fs.FileSetattrer)
	if ok && fsa != nil {
		fsa.Setattr(ctx, in, out)
	} else {
		if m, ok := in.GetMode(); ok {
			if err := syscall.Chmod(path, m); err != nil {
				return fs.ToErrno(err)
			}
		}

		uid, uok := in.GetUID()
		gid, gok := in.GetGID()
		if uok || gok {
			suid := -1
			sgid := -1
			if uok {
				suid = int(uid)
			}
			if gok {
				sgid = int(gid)
			}
			if err := syscall.Chown(path, suid, sgid); err != nil {
				return fs.ToErrno(err)
			}
		}

		mtime, mok := in.GetMTime()
		atime, aok := in.GetATime()

		if mok || aok {
			ap := &atime
			mp := &mtime
			if !aok {
				ap = nil
			}
			if !mok {
				mp = nil
			}
			var ts [2]syscall.Timespec
			ts[0] = fuse.UtimeToTimespec(ap)
			ts[1] = fuse.UtimeToTimespec(mp)

			if err := syscall.UtimesNano(path, ts[:]); err != nil {
				return fs.ToErrno(err)
			}
		}

		if sz, ok := in.GetSize(); ok {
			if err := syscall.Truncate(path, int64(sz)); err != nil {
				return fs.ToErrno(err)
			}
		}
	}

	if fga, ok := f.(fs.FileGetattrer); ok && fga != nil {
		return fga.Getattr(ctx, out)
	}

	var st syscall.Stat_t
	if err := syscall.Lstat(path, &st); err != nil {
		return fs.ToErrno(err)
	}
	out.FromStat(&st)
	return fs.OK
}

func (n *FileNode) Open(ctx context.Context, flags uint32) (fh fs.FileHandle, fuseFlags uint32, errno syscall.Errno) {
	// flags = flags &^ syscall.O_APPEND // TODO: Why is this here?

	f, err := syscall.Open(n.path(), int(flags), 0)
	if err != nil {
		return nil, 0, fs.ToErrno(err)
	}
	return NewFileHandle(f, n.db, n.name), 0, 0
}

func (n *FileNode) Getlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	if f, ok := f.(fs.FileGetlker); ok {
		return f.Getlk(ctx, owner, lk, flags, out)
	}
	return syscall.Errno(fuse.ENOTSUP)
}

func (n *FileNode) Setlk(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	if f, ok := f.(fs.FileSetlker); ok {
		return f.Setlk(ctx, owner, lk, flags)
	}
	log.Printf("setlk not supported on file node: fh=%T", f)
	return syscall.Errno(fuse.ENOTSUP)
}

func (n *FileNode) Setlkw(ctx context.Context, f fs.FileHandle, owner uint64, lk *fuse.FileLock, flags uint32) syscall.Errno {
	if f, ok := f.(fs.FileSetlkwer); ok {
		return f.Setlkw(ctx, owner, lk, flags)
	}
	return syscall.Errno(fuse.ENOTSUP)
}
