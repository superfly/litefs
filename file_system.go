package litefs

import (
	"strings"

	"github.com/hanwen/go-fuse/v2/fuse"
)

var _ fuse.RawFileSystem = (*FileSystem)(nil)

// FileSystem represents a raw interface to the FUSE file system.
type FileSystem struct {
	path   string // mount path
	server *fuse.Server
	store  *Store

	// If true, logs debug information about every FUSE call.
	Debug bool
}

// NewFileSystem returns a new instance of FileSystem.
func NewFileSystem(path string, store *Store) *FileSystem {
	return &FileSystem{store: store}
}

// Path returns the path to the mount point.
func (fs *FileSystem) Path() string { return fs.path }

// Mounts the file system to the mount point.
func (fs *FileSystem) Mount() (err error) {
	// Create FUSE server and mount it.
	fs.server, err = fuse.NewServer(fs, fs.path, &fuse.MountOptions{
		Name:        "litefs",
		Debug:       fs.Debug,
		EnableLocks: true,
	})
	if err != nil {
		return err
	}

	go fs.server.Serve()

	return nil
}

// Wait blocks until the FUSE server closes. Must call Mount() first.
func (fs *FileSystem) Wait() error {
	return fs.server.WaitMount()
}

// Unmount unmounts the file system.
func (fs *FileSystem) Unmount() (err error) {
	if fs.server != nil {
		if e := fs.server.Unmount(); err == nil {
			err = e
		}
	}
	return err
}

// This is called on processing the first request. The
// filesystem implementation can use the server argument to
// talk back to the kernel (through notify methods).
func (fs *FileSystem) Init(server *fuse.Server) {
}

func (fs *FileSystem) String() string { return "litefs" }

func (fs *FileSystem) SetDebug(dbg bool) {}

func (fs *FileSystem) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	return fuse.ENOSYS
}

// Lookup is called by the kernel when the VFS wants to know
// about a file inside a directory. Many lookup calls can
// occur in parallel, but only one call happens for each (dir,
// name) pair.
func (fs *FileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

// Forget is called when the kernel discards entries from its
// dentry cache. This happens on unmount, and when the kernel
// is short on memory. Since it is not guaranteed to occur at
// any moment, and since there is no return value, Forget
// should not do I/O, as there is no channel to report back
// I/O errors.
func (fs *FileSystem) Forget(nodeID, nlookup uint64) {
}

func (fs *FileSystem) GetAttr(cancel <-chan struct{}, input *fuse.GetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Open(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (code fuse.Status) {
	return fuse.OK
}

func (fs *FileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fs *FileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Unlink(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

// GetXAttr reads an extended attribute, and should return the
// number of bytes. If the buffer is too small, return ERANGE,
// with the required buffer size.
func (fs *FileSystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (size uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

// SetAttr writes an extended attribute.
func (fs *FileSystem) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

// ListXAttr lists extended attributes as '\0' delimited byte
// slice, and return the number of bytes. If the buffer is too
// small, return ERANGE, with the required buffer size.
func (fs *FileSystem) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (n uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FileSystem) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Create(cancel <-chan struct{}, input *fuse.CreateIn, name string, out *fuse.CreateOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fs *FileSystem) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) SetLk(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Release(cancel <-chan struct{}, input *fuse.ReleaseIn) {
}

func (fs *FileSystem) Write(cancel <-chan struct{}, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FileSystem) Flush(cancel <-chan struct{}, input *fuse.FlushIn) fuse.Status {
	return fuse.OK
}

func (fs *FileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	return fuse.ENOSYS
}

func (fs *FileSystem) ReleaseDir(input *fuse.ReleaseIn) {
}

func (fs *FileSystem) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fs *FileSystem) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	return fuse.ENOSYS
}

// FileType represents a type of SQLite file.
type FileType int

const (
	// Main database file
	FileTypeDatabase = FileType(iota)

	// Rollback journal
	FileTypeJournal

	// Write-ahead log
	FileTypeWAL

	// Shared memory
	FileTypeSHM
)

// ParseFilename parses a base name into database name & file type parts.
func ParseFilename(name string) (dbName string, fileType FileType) {
	if strings.HasSuffix(name, "-journal") {
		return strings.TrimSuffix(name, "-journal"), FileTypeJournal
	} else if strings.HasSuffix(name, "-wal") {
		return strings.TrimSuffix(name, "-wal"), FileTypeWAL
	} else if strings.HasSuffix(name, "-shm") {
		return strings.TrimSuffix(name, "-shm"), FileTypeSHM
	}
	return name, FileTypeDatabase
}
