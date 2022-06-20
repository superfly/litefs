package litefs

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fuse"
)

var _ fuse.RawFileSystem = (*FileSystem)(nil)

// FileSystem represents a raw interface to the FUSE file system.
type FileSystem struct {
	mu     sync.Mutex
	path   string // mount path
	server *fuse.Server
	store  *Store

	// Manage file handle creation.
	nextFileHandleID uint64
	fileHandles      map[uint64]*FileHandle

	// User ID for all files in the filesystem.
	Uid int

	// Group ID for all files in the filesystem.
	Gid int

	// If true, logs debug information about every FUSE call.
	Debug bool
}

// NewFileSystem returns a new instance of FileSystem.
func NewFileSystem(path string, store *Store) *FileSystem {
	return &FileSystem{
		path:  path,
		store: store,

		nextFileHandleID: 0xff00,
		fileHandles:      make(map[uint64]*FileHandle),

		Uid: os.Getuid(),
		Gid: os.Getgid(),
	}
}

// Path returns the path to the mount point.
func (fs *FileSystem) Path() string { return fs.path }

// Store returns the underlying store.
func (fs *FileSystem) Store() *Store { return fs.store }

// Mount mounts the file system to the mount point.
func (fs *FileSystem) Mount() (err error) {
	// Create FUSE server and mount it.
	fs.server, err = fuse.NewServer(fs, fs.path, &fuse.MountOptions{
		Name:  "litefs",
		Debug: fs.Debug,
		// EnableLocks: true,
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

func (fs *FileSystem) Lookup(cancel <-chan struct{}, header *fuse.InHeader, name string, out *fuse.EntryOut) (code fuse.Status) {
	// Ensure lookup is only performed on top-level directory.
	if header.NodeId != rootNodeID {
		log.Printf("fuse: lookup(): invalid inode: %d", header.NodeId)
		return fuse.EINVAL
	}

	dbName, fileType := ParseFilename(name)
	db := fs.store.FindDBByName(dbName)
	if db == nil {
		return fuse.ENOENT
	}

	attr, err := fs.dbFileAttr(db, fileType)
	if os.IsNotExist(err) {
		return fuse.ENOENT
	} else if err != nil {
		log.Printf("fuse: lookup(): attr error: %s", err)
		return fuse.EIO
	}

	out.NodeId = attr.Ino
	out.Generation = 1
	out.Attr = attr
	return fuse.OK
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
	// Handle root directory.
	if input.NodeId == rootNodeID {
		out.Attr = fuse.Attr{
			Ino:     rootNodeID,
			Mode:    040777,
			Nlink:   1,
			Blksize: 4096,
			Owner: fuse.Owner{
				Uid: uint32(fs.Uid),
				Gid: uint32(fs.Gid),
			},
		}
		return fuse.OK
	}

	dbID, fileType, err := ParseInode(input.NodeId)
	if err != nil {
		log.Printf("fuse: getattr(): cannot parse inode: %d", input.NodeId)
		return fuse.ENOENT
	}

	db := fs.store.FindDB(dbID)
	if db == nil {
		return fuse.ENOENT
	}

	attr, err := fs.dbFileAttr(db, fileType)
	if os.IsNotExist(err) {
		return fuse.ENOENT
	} else if err != nil {
		log.Printf("fuse: getattr(): attr error: %s", err)
		return fuse.EIO
	}

	out.Attr = attr
	return fuse.OK
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

func (fs *FileSystem) Unlink(cancel <-chan struct{}, input *fuse.InHeader, name string) (code fuse.Status) {
	// Ensure command is only performed on top-level directory.
	if input.NodeId != rootNodeID {
		log.Printf("fuse: unlink(): invalid parent inode: %d", input.NodeId)
		return fuse.EINVAL
	}

	dbName, fileType := ParseFilename(name)

	switch fileType {
	case FileTypeDatabase:
		return fs.unlinkDatabase(cancel, input, dbName)
	case FileTypeJournal:
		return fs.unlinkJournal(cancel, input, dbName)
	case FileTypeWAL:
		return fs.unlinkWAL(cancel, input, dbName)
	case FileTypeSHM:
		return fs.unlinkSHM(cancel, input, dbName)
	default:
		return fuse.EINVAL
	}
}

func (fs *FileSystem) unlinkDatabase(cancel <-chan struct{}, input *fuse.InHeader, dbName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) unlinkJournal(cancel <-chan struct{}, input *fuse.InHeader, dbName string) (code fuse.Status) {
	db := fs.store.FindDBByName(dbName)
	if db == nil {
		return fuse.ENOENT
	}

	if err := db.UnlinkJournal(); err != nil {
		log.Printf("fuse: unlink(): cannot delete journal: %s", err)
		return toErrno(err)
	}
	return fuse.OK
}

func (fs *FileSystem) unlinkWAL(cancel <-chan struct{}, input *fuse.InHeader, dbName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) unlinkSHM(cancel <-chan struct{}, input *fuse.InHeader, dbName string) (code fuse.Status) {
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
	if input.NodeId != rootNodeID {
		log.Printf("fuse: lookup(): invalid inode: %d", input.NodeId)
		return fuse.EINVAL
	}

	dbName, fileType := ParseFilename(name)

	switch fileType {
	case FileTypeDatabase:
		return fs.createDatabase(cancel, input, dbName, out)
	case FileTypeJournal:
		return fs.createJournal(cancel, input, dbName, out)
	case FileTypeWAL:
		return fs.createWAL(cancel, input, dbName, out)
	case FileTypeSHM:
		return fs.createSHM(cancel, input, dbName, out)
	default:
		return fuse.EINVAL
	}
}

func (fs *FileSystem) createDatabase(cancel <-chan struct{}, input *fuse.CreateIn, dbName string, out *fuse.CreateOut) (code fuse.Status) {
	db, file, err := fs.store.CreateDB(dbName)
	if err == ErrDatabaseExists {
		return fuse.Status(syscall.EEXIST)
	} else if err != nil {
		log.Printf("fuse: create(): cannot create database: %s", err)
		return toErrno(err)
	}

	attr, err := fs.dbFileAttr(db, FileTypeDatabase)
	if err != nil {
		log.Printf("fuse: create(): cannot stat database file: %s", err)
		return toErrno(err)
	}

	ino := fs.dbIno(db.ID(), FileTypeDatabase)
	fh := fs.NewFileHandle(db, FileTypeDatabase, file)
	out.Fh = fh.ID()
	out.NodeId = ino
	out.Attr = attr

	return fuse.OK
}

func (fs *FileSystem) createJournal(cancel <-chan struct{}, input *fuse.CreateIn, dbName string, out *fuse.CreateOut) (code fuse.Status) {
	db := fs.store.FindDBByName(dbName)
	if db == nil {
		log.Printf("fuse: create(): cannot create journal, database not found: %s", dbName)
		return fuse.Status(syscall.ENOENT)
	}

	file, err := db.CreateJournal()
	if err != nil {
		log.Printf("fuse: create(): cannot find journal: %s", err)
		return toErrno(err)
	}

	attr, err := fs.dbFileAttr(db, FileTypeJournal)
	if err != nil {
		log.Printf("fuse: create(): cannot stat journal file: %s", err)
		return toErrno(err)
	}

	ino := fs.dbIno(db.ID(), FileTypeJournal)
	fh := fs.NewFileHandle(db, FileTypeJournal, file)
	out.Fh = fh.ID()
	out.NodeId = ino
	out.Attr = attr

	return fuse.OK
}

func (fs *FileSystem) createWAL(cancel <-chan struct{}, input *fuse.CreateIn, dbName string, out *fuse.CreateOut) (code fuse.Status) {
	return fuse.ENOSYS // TODO
}

func (fs *FileSystem) createSHM(cancel <-chan struct{}, input *fuse.CreateIn, dbName string, out *fuse.CreateOut) (code fuse.Status) {
	return fuse.ENOSYS // TODO
}

func (fs *FileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	return fuse.ENOSYS
}

func (fs *FileSystem) Read(cancel <-chan struct{}, input *fuse.ReadIn, buf []byte) (fuse.ReadResult, fuse.Status) {
	fh := fs.FileHandle(input.Fh)
	if fh == nil {
		log.Printf("fuse: read(): bad file handle: %d", input.Fh)
		return nil, fuse.EBADF
	}

	//println("dbg/read", len(buf))
	//n, err := fh.File().ReadAt(buf, int64(input.Offset))
	//if err == io.EOF {
	//	println("dbg/read.eof")
	//	return fuse.ReadResultData(nil), fuse.OK
	//} else if err != nil {
	//	log.Printf("fuse: read(): cannot read: %s", err)
	//	return nil, fuse.EIO
	//}
	//return fuse.ReadResultData(buf[:n]), fuse.OK

	return fuse.ReadResultFd(fh.File().Fd(), int64(input.Offset), int(input.Size)), fuse.OK
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
	fh := fs.FileHandle(input.Fh)
	if fh == nil {
		log.Printf("fuse: write(): invalid file handle: %d", input.Fh)
		return 0, fuse.EBADF
	}

	switch fh.FileType() {
	case FileTypeDatabase:
		return fs.writeDatabase(cancel, fh, input, data)
	case FileTypeJournal:
		return fs.writeJournal(cancel, fh, input, data)
	case FileTypeWAL:
		return fs.writeWAL(cancel, fh, input, data)
	case FileTypeSHM:
		return fs.writeSHM(cancel, fh, input, data)
	default:
		log.Printf("fuse: write(): file handle has invalid file type: %d", fh.FileType())
		return 0, fuse.EINVAL
	}
}

func (fs *FileSystem) writeDatabase(cancel <-chan struct{}, fh *FileHandle, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	if err := fh.DB().WriteDatabase(fh.File(), data, int64(input.Offset)); err != nil {
		return 0, toErrno(err)
	}
	return uint32(len(data)), fuse.OK
}

func (fs *FileSystem) writeJournal(cancel <-chan struct{}, fh *FileHandle, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	if err := fh.DB().WriteJournal(fh.File(), data, int64(input.Offset)); err != nil {
		return 0, toErrno(err)
	}
	return uint32(len(data)), fuse.OK
}

func (fs *FileSystem) writeWAL(cancel <-chan struct{}, fh *FileHandle, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS // TODO
}

func (fs *FileSystem) writeSHM(cancel <-chan struct{}, fh *FileHandle, input *fuse.WriteIn, data []byte) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS // TODO
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

// dbIno returns the inode for a given database's file.
func (fs FileSystem) dbIno(dbID uint64, fileType FileType) uint64 {
	return (uint64(dbID) << 4) | fileType.ino()
}

// dbFileAttr returns an attribute for a given database file.
func (fs FileSystem) dbFileAttr(db *DB, fileType FileType) (fuse.Attr, error) {
	// Look up stats on the internal data file. May return "not found".
	fi, err := os.Stat(filepath.Join(db.Path(), fileType.filename()))
	if err != nil {
		return fuse.Attr{}, err
	}

	t := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	return fuse.Attr{
		Ino:     fs.dbIno(db.ID(), fileType),
		Size:    uint64(fi.Size()),
		Atime:   uint64(t.Unix()),
		Mtime:   uint64(t.Unix()),
		Ctime:   uint64(t.Unix()),
		Mode:    0100666,
		Nlink:   1,
		Blksize: 4096,
		Owner: fuse.Owner{
			Uid: uint32(fs.Uid),
			Gid: uint32(fs.Gid),
		},
	}, nil
}

// NewFileHandle returns a new file handle associated with a database file.
func (fs *FileSystem) NewFileHandle(db *DB, fileType FileType, file *os.File) *FileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fileHandle := &FileHandle{
		id:       fs.nextFileHandleID,
		db:       db,
		fileType: fileType,
		file:     file,
	}
	fs.nextFileHandleID++

	fs.fileHandles[fileHandle.ID()] = fileHandle
	return fileHandle
}

// FileHandle returns a file handle by ID.
func (fs *FileSystem) FileHandle(id uint64) *FileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.fileHandles[id]
}

// FileHandle represents a file system handle that points to a database file.
type FileHandle struct {
	id       uint64
	db       *DB
	fileType FileType
	file     *os.File
}

// ID returns the file handle identifier.
func (fh *FileHandle) ID() uint64 { return fh.id }

// DB returns the database associated with the file handle.
func (fh *FileHandle) DB() *DB { return fh.db }

// FileType return the type of database file the handle is associated with.
func (fh *FileHandle) FileType() FileType { return fh.fileType }

// File return the underlying file reference.
func (fh *FileHandle) File() *os.File { return fh.file }

// ID returns the file handle identifier.
func (fh *FileHandle) Close() (err error) {
	if fh.file != nil {
		return fh.file.Close()
	}
	return nil
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

// IsValid returns true if t is a valid file type.
func (t FileType) IsValid() bool {
	switch t {
	case FileTypeDatabase, FileTypeJournal, FileTypeWAL, FileTypeSHM:
		return true
	default:
		return false
	}
}

// filename returns the base name for the internal data file.
func (t FileType) filename() string {
	switch t {
	case FileTypeDatabase:
		return "database"
	case FileTypeJournal:
		return "journal"
	case FileTypeWAL:
		return "wal"
	case FileTypeSHM:
		return "shm"
	default:
		panic(fmt.Sprintf("FileType.filename(): invalid file type: %d", t))
	}
}

// ino returns the inode offset for the file type.
func (t FileType) ino() uint64 {
	switch t {
	case FileTypeDatabase:
		return 0
	case FileTypeJournal:
		return 1
	case FileTypeWAL:
		return 2
	case FileTypeSHM:
		return 3
	default:
		panic(fmt.Sprintf("FileType.ino(): invalid file type: %d", t))
	}
}

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

// ParseInode parses an inode into its database ID & file type parts.
func ParseInode(ino uint64) (dbID uint64, fileType FileType, err error) {
	if ino < 1<<4 {
		return 0, 0, fmt.Errorf("invalid inode, out of range: %d", ino)
	}

	dbID = ino >> 4
	fileType = FileType(ino & 0xF)
	if !fileType.IsValid() {
		return 0, 0, fmt.Errorf("invalid file type: ino=%d file_type=%d", ino, fileType)
	}
	return dbID, fileType, nil
}

// toErrno converts an error to a FUSE status code.
func toErrno(err error) fuse.Status {
	if err == nil {
		return fuse.OK
	} else if os.IsNotExist(err) {
		return fuse.ENOENT
	}
	return fuse.EPERM
}

// rootNodeID is the identifier of the top-level directory.
const rootNodeID = 1
