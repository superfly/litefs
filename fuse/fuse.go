package fuse

import (
	"fmt"
	"math"
	"os"
	"strings"
	"syscall"

	"bazil.org/fuse"
	"github.com/superfly/litefs"
)

//----------------------------------
/*
func (fsys *FileSystem) Fsync(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	fh := fs.FileHandle(input.Fh)
	if fh == nil {
		log.Printf("fuse: fsync(): bad file handle: %d", input.Fh)
		return fuse.EBADF
	}

	if err := fh.File().Sync(); err != nil {
		log.Printf("fuse: fsync(): cannot sync: %s", err)
		return toErrno(err)
	}
	return fuse.OK
}

func (fsys *FileSystem) GetLk(cancel <-chan struct{}, in *fuse.LkIn, out *fuse.LkOut) (code fuse.Status) {
	fh := fs.FileHandle(in.Fh)
	if fh == nil {
		log.Printf("fuse: setlk(): bad file handle: %d", in.Fh)
		return fuse.EBADF
	}

	// If a lock could not be obtained, return a write lock in its place.
	// This isn't technically correct but it's good enough for SQLite usage.
	if ok, err := fh.GetLk(in.Lk.Typ, litefs.ParseLockRange(in.Lk.Start, in.Lk.End)); err != nil {
		log.Printf("fuse: getlk(): error: %s", err)
		return fuse.ENOSYS
	} else if !ok {
		out.Lk = fuse.FileLock{
			Start: in.Lk.Start,
			End:   in.Lk.End,
			Typ:   syscall.F_WRLCK,
		}
		return fuse.OK
	}

	// If lock could be obtained, return UNLCK.
	out.Lk = fuse.FileLock{
		Start: in.Lk.Start,
		End:   in.Lk.End,
		Typ:   syscall.F_UNLCK,
	}
	return fuse.OK
}

func (fsys *FileSystem) SetLk(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	fh := fs.FileHandle(in.Fh)
	if fh == nil {
		log.Printf("fuse: setlk(): bad file handle: %d", in.Fh)
		return fuse.EBADF
	}

	if ok, err := fh.SetLk(in.Lk.Typ, litefs.ParseLockRange(in.Lk.Start, in.Lk.End)); err != nil {
		log.Printf("fuse: setlk(): error: %s", err)
		return fuse.ENOSYS
	} else if !ok {
		return fuse.EAGAIN
	}
	return fuse.OK
}

func (fsys *FileSystem) SetLkw(cancel <-chan struct{}, in *fuse.LkIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) OpenDir(cancel <-chan struct{}, input *fuse.OpenIn, out *fuse.OpenOut) (status fuse.Status) {
	out.Fh = fs.NewRootHandle().ID()
	out.OpenFlags = input.Flags
	return fuse.OK
}

func (fsys *FileSystem) ReadDirPlus(cancel <-chan struct{}, input *fuse.ReadIn, out *fuse.DirEntryList) fuse.Status {
	h := fs.RootHandle(input.Fh)
	if h == nil {
		log.Printf("fuse: readdirplus(): bad file handle: %d", input.Fh)
		return fuse.EBADF
	}

	// Build hidden, fixed entries list first.
	var entries []fuse.DirEntry
	if primaryURL := fs.store.PrimaryURL(); primaryURL != "" {
		entries = append(entries, fuse.DirEntry{
			Name: PrimaryFilename,
			Ino:  PrimaryNodeID,
			Mode: 0100444,
		})
	}

	// Write out entries until we fill the buffer.
	var index int
	for _, entry := range entries {
		if index < h.offset {
			continue
		}

		if out.AddDirLookupEntry(entry) == nil {
			return fuse.OK
		}
		index++
		h.offset++
	}

	// Read & sort list of databases from the store.
	dbs := fs.store.DBs()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })

	// Iterate over databases starting from the offset.
	for _, db := range dbs {
		if index < h.offset {
			continue
		}

		// Write the entry to the buffer; if nil returned then buffer is full.
		if out.AddDirLookupEntry(fuse.DirEntry{
			Name: db.Name(),
			Ino:  fs.dbIno(db.ID(), litefs.FileTypeDatabase),
			Mode: 0100666},
		) == nil {
			break
		}

		index++
		h.offset++
	}
	return fuse.OK
}

func (fsys *FileSystem) ReadDir(cancel <-chan struct{}, input *fuse.ReadIn, l *fuse.DirEntryList) fuse.Status {
	return fuse.ENOSYS
}

func (fsys *FileSystem) ReleaseDir(input *fuse.ReleaseIn) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.RootHandles, input.Fh)
}

func (fsys *FileSystem) FsyncDir(cancel <-chan struct{}, input *fuse.FsyncIn) (code fuse.Status) {
	return fuse.OK
}

func (fsys *FileSystem) Fallocate(cancel <-chan struct{}, in *fuse.FallocateIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) CopyFileRange(cancel <-chan struct{}, input *fuse.CopyFileRangeIn) (written uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fsys *FileSystem) Lseek(cancel <-chan struct{}, in *fuse.LseekIn, out *fuse.LseekOut) fuse.Status {
	return fuse.ENOSYS
}

func (fsys *FileSystem) SetAttr(cancel <-chan struct{}, input *fuse.SetAttrIn, out *fuse.AttrOut) (code fuse.Status) {
	fh := fs.FileHandle(input.Fh)
	if fh == nil {
		log.Printf("fuse: setattr(): bad file handle: %d", input.Fh)
		return fuse.EBADF
	}

	switch fileType := fh.FileType(); fileType {
	case litefs.FileTypeDatabase:
		return fs.setAttrDatabase(cancel, input, fh, out)
	case litefs.FileTypeJournal:
		return fs.setAttrJournal(cancel, input, fh, out)
	case litefs.FileTypeWAL:
		return fs.setAttrWAL(cancel, input, fh, out)
	case litefs.FileTypeSHM:
		return fs.setAttrSHM(cancel, input, fh, out)
	default:
		log.Printf("fuse: setattr(): invalid file handle type: %d", fileType)
		return fuse.ENOENT
	}
}

func (fsys *FileSystem) setAttrDatabase(cancel <-chan struct{}, input *fuse.SetAttrIn, fh *FileHandle, out *fuse.AttrOut) (code fuse.Status) {
	return fuse.EPERM
}

func (fsys *FileSystem) setAttrJournal(cancel <-chan struct{}, input *fuse.SetAttrIn, fh *FileHandle, out *fuse.AttrOut) (code fuse.Status) {
	if input.Size != 0 {
		log.Printf("fuse: setattr(): size must be zero when truncating journal: sz=%d", input.Size)
		return fuse.EPERM
	}

	if err := fh.DB().CommitJournal(litefs.JournalModeTruncate); err != nil {
		log.Printf("fuse: setattr(): cannot truncate journal: %s", err)
		return fuse.EIO
	}

	attr, err := fs.dbFileAttr(fh.DB(), fh.FileType())
	if os.IsNotExist(err) {
		return fuse.ENOENT
	} else if err != nil {
		log.Printf("fuse: setattr(): attr error: %s", err)
		return fuse.EIO
	}
	out.Attr = attr

	return fuse.OK
}

func (fsys *FileSystem) setAttrWAL(cancel <-chan struct{}, input *fuse.SetAttrIn, fh *FileHandle, out *fuse.AttrOut) (code fuse.Status) {
	return fuse.EPERM
}

func (fsys *FileSystem) setAttrSHM(cancel <-chan struct{}, input *fuse.SetAttrIn, fh *FileHandle, out *fuse.AttrOut) (code fuse.Status) {
	return fuse.EPERM
}

func (fsys *FileSystem) Readlink(cancel <-chan struct{}, header *fuse.InHeader) (out []byte, code fuse.Status) {
	return nil, fuse.ENOSYS
}

func (fsys *FileSystem) Mknod(cancel <-chan struct{}, input *fuse.MknodIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Mkdir(cancel <-chan struct{}, input *fuse.MkdirIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Rmdir(cancel <-chan struct{}, header *fuse.InHeader, name string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Symlink(cancel <-chan struct{}, header *fuse.InHeader, pointedTo string, linkName string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Rename(cancel <-chan struct{}, input *fuse.RenameIn, oldName string, newName string) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Link(cancel <-chan struct{}, input *fuse.LinkIn, name string, out *fuse.EntryOut) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) GetXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string, dest []byte) (size uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fsys *FileSystem) SetXAttr(cancel <-chan struct{}, input *fuse.SetXAttrIn, attr string, data []byte) fuse.Status {
	return fuse.ENOSYS
}

// ListXAttr lists extended attributes as '\0' delimited byte
// slice, and return the number of bytes. If the buffer is too
// small, return ERANGE, with the required buffer size.
func (fsys *FileSystem) ListXAttr(cancel <-chan struct{}, header *fuse.InHeader, dest []byte) (n uint32, code fuse.Status) {
	return 0, fuse.ENOSYS
}

func (fsys *FileSystem) RemoveXAttr(cancel <-chan struct{}, header *fuse.InHeader, attr string) fuse.Status {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Access(cancel <-chan struct{}, input *fuse.AccessIn) (code fuse.Status) {
	return fuse.ENOSYS
}

func (fsys *FileSystem) StatFs(cancel <-chan struct{}, header *fuse.InHeader, out *fuse.StatfsOut) fuse.Status {
	return fuse.ENOSYS
}

func (fsys *FileSystem) Forget(nodeID, nlookup uint64) {}

// dbIno returns the inode for a given database's file.
func (fs FileSystem) dbIno(dbID uint32, fileType litefs.FileType) uint64 {
	return (uint64(dbID) << 4) | FileTypeInode(fileType)
}

// dbFileAttr returns an attribute for a given database file.
func (fs FileSystem) dbFileAttr(db *litefs.DB, fileType litefs.FileType) (fuse.Attr, error) {
	// Look up stats on the internal data file. May return "not found".
	fi, err := os.Stat(filepath.Join(db.Path(), FileTypeFilename(fileType)))
	if err != nil {
		return fuse.Attr{}, err
	}

	// Mask mode if it is not writable.
	mode := uint32(0100666)
	if !fs.store.IsPrimary() {
		mode = uint32(0100444)
	}

	t := time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)
	return fuse.Attr{
		Ino:     fs.dbIno(db.ID(), fileType),
		Size:    uint64(fi.Size()),
		Atime:   uint64(t.Unix()),
		Mtime:   uint64(t.Unix()),
		Ctime:   uint64(t.Unix()),
		Mode:    mode,
		Nlink:   1,
		Blksize: 4096,
		Owner: fuse.Owner{
			Uid: uint32(fs.Uid),
			Gid: uint32(fs.Gid),
		},
	}, nil
}

// NewFileHandle returns a new file handle associated with a database file.
func (fsys *FileSystem) NewFileHandle(db *litefs.DB, fileType litefs.FileType, file *os.File) *FileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	fh := NewFileHandle(fs.nextHandleID, db, fileType, file)
	fs.nextHandleID++
	fs.fileHandles[fh.ID()] = fh

	return fh
}

// FileHandle returns a file handle by ID.
func (fsys *FileSystem) FileHandle(id uint64) *FileHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.fileHandles[id]
}

// NewRootHandle returns a new directory handle associated with the root directory.
func (fsys *FileSystem) NewRootHandle() *RootHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()

	h := NewRootHandle(fs.nextHandleID)
	fs.nextHandleID++
	fs.RootHandles[h.ID()] = h
	return h
}

// RootHandle returns a directory handle by ID.
func (fsys *FileSystem) RootHandle(id uint64) *RootHandle {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	return fs.RootHandles[id]
}

// FileHandle represents a file system handle that points to a database file.
type FileHandle struct {
	id       uint64
	db       *litefs.DB
	fileType litefs.FileType
	file     *os.File

	// SQLite locks held
	pendingGuard  *litefs.RWMutexGuard
	sharedGuard   *litefs.RWMutexGuard
	reservedGuard *litefs.RWMutexGuard
}

// NewFileHandle returns a new instance of FileHandle.
func NewFileHandle(id uint64, db *litefs.DB, fileType litefs.FileType, file *os.File) *FileHandle {
	return &FileHandle{
		id:       id,
		db:       db,
		fileType: fileType,
		file:     file,
	}
}

// ID returns the file handle identifier.
func (fh *FileHandle) ID() uint64 { return fh.id }

// DB returns the database associated with the file handle.
func (fh *FileHandle) DB() *litefs.DB { return fh.db }

// FileType return the type of database file the handle is associated with.
func (fh *FileHandle) FileType() litefs.FileType { return fh.fileType }

// File return the underlying file reference.
func (fh *FileHandle) File() *os.File { return fh.file }

// ID returns the file handle identifier.
func (fh *FileHandle) Close() (err error) {
	if fh.file != nil {
		return fh.file.Close()
	}
	return nil
}

// mutexAndGuardRefByLockType returns the mutex and, if held, a guard for that mutex.
func (fh *FileHandle) mutexAndGuardRefByLockType(lockType litefs.LockType) (mu *litefs.RWMutex, guardRef **litefs.RWMutexGuard, err error) {
	switch lockType {
	case litefs.LockTypePending:
		return fh.db.PendingLock(), &fh.pendingGuard, nil
	case litefs.LockTypeReserved:
		return fh.db.ReservedLock(), &fh.reservedGuard, nil
	case litefs.LockTypeShared:
		return fh.db.SharedLock(), &fh.sharedGuard, nil
	default:
		return nil, nil, fmt.Errorf("invalid lock type: %d", lockType)
	}
}

// Getlk returns true if one or more locks could be obtained.
// This function does not actually acquire the locks.
func (fh *FileHandle) GetLk(typ uint32, lockTypes []litefs.LockType) (bool, error) {
	for _, lockType := range lockTypes {
		if ok, err := fh.getLk(typ, lockType); err != nil {
			return false, err
		} else if !ok {
			return false, nil
		}
	}
	return true, nil
}

func (fh *FileHandle) getLk(typ uint32, lockType litefs.LockType) (bool, error) {
	// TODO: Hold file handle lock

	mu, guardRef, err := fh.mutexAndGuardRefByLockType(lockType)
	if err != nil {
		return false, err
	}

	if *guardRef != nil {
		switch typ {
		case syscall.F_UNLCK:
			return true, nil
		case syscall.F_RDLCK:
			return true, nil
		case syscall.F_WRLCK:
			return (*guardRef).CanLock(), nil
		default:
			panic(fmt.Sprintf("invalid posix lock type: %d", typ))
		}
	}

	switch typ {
	case syscall.F_UNLCK:
		return true, nil
	case syscall.F_RDLCK:
		return mu.CanRLock(), nil
	case syscall.F_WRLCK:
		return mu.CanLock(), nil
	default:
		panic(fmt.Sprintf("invalid posix lock type: %d", typ))
	}
}

// SetLk transition a lock to a new state. Only UNLCK can be performed against
// multiple locks. Returns false if not all locks can be transitioned.
func (fh *FileHandle) SetLk(typ uint32, lockTypes []litefs.LockType) (bool, error) {
	if len(lockTypes) == 0 {
		return false, fmt.Errorf("no locks")
	}

	// Handle unlock separately since it can handle multiple locks at once.
	if typ == syscall.F_UNLCK {
		return true, fh.setUnlk(lockTypes)
	} else if len(lockTypes) > 1 {
		return false, fmt.Errorf("cannot acquire multiple locks at once")
	}
	lockType := lockTypes[0]

	// TODO: Hold file handle lock for rest of function.
	mu, guardRef, err := fh.mutexAndGuardRefByLockType(lockType)
	if err != nil {
		return false, err
	}

	if *guardRef != nil {
		switch typ {
		case syscall.F_RDLCK:
			(*guardRef).RLock()
			return true, nil
		case syscall.F_WRLCK:
			return (*guardRef).TryLock(), nil
		default:
			panic(fmt.Sprintf("invalid posix lock type: %d", typ))
		}
	}

	switch typ {
	case syscall.F_RDLCK:
		*guardRef = mu.TryRLock()
		return *guardRef != nil, nil
	case syscall.F_WRLCK:
		*guardRef = mu.TryLock()
		return *guardRef != nil, nil
	default:
		panic(fmt.Sprintf("invalid posix lock type: %d", typ))
	}
}

func (fh *FileHandle) setUnlk(lockTypes []litefs.LockType) error {
	for _, lockType := range lockTypes {
		_, guardRef, err := fh.mutexAndGuardRefByLockType(lockType)
		if err != nil {
			return err
		} else if *guardRef == nil {
			continue // no lock acquired, skip
		}

		// Unlock and drop reference to the guard.
		(*guardRef).Unlock()
		*guardRef = nil
	}
	return nil
}
*/

// FileTypeFilename returns the base name for the internal data file.
func FileTypeFilename(t litefs.FileType) string {
	switch t {
	case litefs.FileTypeDatabase:
		return "database"
	case litefs.FileTypeJournal:
		return "journal"
	case litefs.FileTypeWAL:
		return "wal"
	case litefs.FileTypeSHM:
		return "shm"
	default:
		panic(fmt.Sprintf("FileTypeFilename(): invalid file type: %d", t))
	}
}

// FileTypeInode returns the inode offset for the file type.
func FileTypeInode(t litefs.FileType) uint64 {
	switch t {
	case litefs.FileTypeDatabase:
		return 0
	case litefs.FileTypeJournal:
		return 1
	case litefs.FileTypeWAL:
		return 2
	case litefs.FileTypeSHM:
		return 3
	default:
		panic(fmt.Sprintf("FileTypeInode(): invalid file type: %d", t))
	}
}

// FileTypeFromInode returns the file type for the given inode offset.
func FileTypeFromInode(ino uint64) (litefs.FileType, error) {
	switch ino {
	case 0:
		return litefs.FileTypeDatabase, nil
	case 1:
		return litefs.FileTypeJournal, nil
	case 2:
		return litefs.FileTypeWAL, nil
	case 3:
		return litefs.FileTypeSHM, nil
	default:
		return litefs.FileTypeNone, fmt.Errorf("invalid inode file type: %d", ino)
	}
}

// ParseFilename parses a base name into database name & file type parts.
func ParseFilename(name string) (dbName string, fileType litefs.FileType) {
	if strings.HasSuffix(name, "-journal") {
		return strings.TrimSuffix(name, "-journal"), litefs.FileTypeJournal
	} else if strings.HasSuffix(name, "-wal") {
		return strings.TrimSuffix(name, "-wal"), litefs.FileTypeWAL
	} else if strings.HasSuffix(name, "-shm") {
		return strings.TrimSuffix(name, "-shm"), litefs.FileTypeSHM
	}
	return name, litefs.FileTypeDatabase
}

// ParseInode parses an inode into its database ID & file type parts.
func ParseInode(ino uint64) (dbID uint32, fileType litefs.FileType, err error) {
	if ino < 1<<4 {
		return 0, 0, fmt.Errorf("invalid inode, out of range: %d", ino)
	}

	dbID64 := ino >> 4
	if dbID64 > math.MaxUint32 {
		return 0, 0, fmt.Errorf("inode overflows database id")
	}

	fileType, err = FileTypeFromInode(ino & 0xF)
	if err != nil {
		return 0, 0, err
	}
	return uint32(dbID64), fileType, nil
}

// toErrno converts an error to a FUSE status code.
func toErrno(err error) error {
	if err == nil {
		return nil
	} else if os.IsNotExist(err) {
		return fuse.Errno(syscall.ENOENT)
	} else if err == litefs.ErrReadOnlyReplica {
		return fuse.Errno(syscall.EROFS)
	}
	return fuse.Errno(syscall.EIO)
}

const (
	// RootNodeID is the identifier of the top-level directory.
	RootNodeID = 1

	// PrimaryNodeID is the identifier of the ".primary" file.
	PrimaryNodeID = 2
)

const (
	// PrimaryFilename is the name of the file that holds the current primary
	PrimaryFilename = ".primary"
)

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
