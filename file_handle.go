package litefs

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	"github.com/liteserver/liteserver"
	"golang.org/x/sys/unix"
)

var _ fs.FileHandle = (*FileHandle)(nil)
var _ fs.FileReleaser = (*FileHandle)(nil)
var _ fs.FileGetattrer = (*FileHandle)(nil)
var _ fs.FileReader = (*FileHandle)(nil)
var _ fs.FileWriter = (*FileHandle)(nil)
var _ fs.FileGetlker = (*FileHandle)(nil)
var _ fs.FileSetlker = (*FileHandle)(nil)
var _ fs.FileSetlkwer = (*FileHandle)(nil)
var _ fs.FileLseeker = (*FileHandle)(nil)
var _ fs.FileFlusher = (*FileHandle)(nil)
var _ fs.FileFsyncer = (*FileHandle)(nil)
var _ fs.FileAllocater = (*FileHandle)(nil)

// var _ FileSetattrer = (*FileHandle)(nil)

type FileHandle struct {
	mu   sync.Mutex
	fd   int
	db   *DB
	name string

	tx     *liteserver.Tx
	fhdr   []byte
	offset int64
	size   int64
	pgnos  []uint32
}

func NewFileHandle(fd int, db *DB, name string) *FileHandle {
	return &FileHandle{
		fd:   fd,
		db:   db,
		name: name,
	}
}

// IsSHM returns true if this is a handle for the shared memory file.
func (f *FileHandle) IsSHM() bool {
	return strings.HasSuffix(f.name, "-shm")
}

// IsWAL returns true if this is a handle for the write ahead log file.
func (f *FileHandle) IsWAL() bool {
	return strings.HasSuffix(f.name, "-wal")
}

func (f *FileHandle) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	r := fuse.ReadResultFd(uintptr(f.fd), off, len(buf))
	return r, fs.OK
}

func (f *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()

	n, err := syscall.Pwrite(f.fd, data, off)
	if err != nil {
		return uint32(n), fs.ToErrno(err)
	}

	if f.IsWAL() {
		switch len(data) {
		case WALHeaderSize:
			assert(off == 0, "misaligned wal header") // no-op, skip
		case WALFrameHeaderSize:
			f.fhdr = make([]byte, WALFrameHeaderSize)
			copy(f.fhdr, data)
		case PageSize:
			if err := f.writeWALPage(ctx, data, off); err != nil {
				log.Printf("cannot write wal page: offset=%d size=%d err=%s", off, len(data), err)
				return 0, syscall.EIO
			}
		}
	}

	return uint32(n), fs.OK
}

func (f *FileHandle) writeWALPage(ctx context.Context, data []byte, off int64) error {
	log.Printf("write wal page: len=%d off=%d", len(data), off)

	if f.fhdr == nil {
		return fmt.Errorf("unexpected page write without frame header")
	}

	// Require one page at a time.
	if len(data) != PageSize {
		return fmt.Errorf("unexpected wal page size: %d", len(data))
	}

	// Initialize transaction, if first page write.
	if f.tx == nil {
		f.offset, f.size = off-int64(len(f.fhdr)), 0
		f.pgnos = f.pgnos[:0]

		if err := f.begin(); err != nil {
			return fmt.Errorf("begin: %w", err)
		}
	}

	// TODO: Validate contiguous writes.

	// Read frame header data.
	pgno := binary.BigEndian.Uint32(f.fhdr[0:])
	commit := binary.BigEndian.Uint32(f.fhdr[4:])

	log.Printf("write tx page: pgno=%d commit=%d", pgno, commit)

	// Append to transaction.
	f.pgnos = append(f.pgnos, pgno)
	f.size += int64(len(f.fhdr) + len(data))

	// Exit if this is not the last page in transaction.
	if commit == 0 {
		return nil
	}

	log.Printf("commiting tx: offset=%d size=%d pgnos=%v commit=%d", f.offset, f.size, f.pgnos, commit)

	// Flush write to server.
	buf, err := readTxBuffer(f.db.path()+"-wal", f.offset, f.size)
	if err != nil {
		return fmt.Errorf("cannot read tx buffer: off=%d sz=%d err=%w", f.offset, f.size, err)
	}
	binary.BigEndian.PutUint32(buf[len(buf)-4:], commit) // write commit to end

	tid, err := f.tx.WriteBody(buf, uint64(commit))
	if err != nil {
		return fmt.Errorf("write tx body: %w", err)
	}
	if err := f.db.SetTID(tid); err != nil {
		return fmt.Errorf("set tid: %w", err)
	}

	if err := f.tx.Close(); err != nil {
		return fmt.Errorf("tx close: %w", err)
	}
	f.tx = nil

	log.Printf("tx committed: tid=%d", tid)

	return nil
}

func (f *FileHandle) begin() (err error) {
	// TODO: Ensure tx not in progress

	log.Printf("begin transaction")

	client := liteserver.NewClient(f.db.root.URL)

	tid, err := f.db.TID()
	if err != nil {
		return fmt.Errorf("tid: %w", err)
	}

	if f.tx, err = client.Begin(tid); err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() {
		if err != nil && f.tx != nil {
			f.tx.Close()
			f.tx = nil
		}
	}()

	if resp, err := f.tx.ReadHeader(); err != nil {
		return fmt.Errorf("read tx header: %w", err)
	} else if resp.TID != tid {
		return fmt.Errorf("tid mismatch: %d <> %d", resp.TID, tid)
	}

	return nil
}

func (f *FileHandle) Release(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.fd == -1 {
		return syscall.EBADF
	}

	err := syscall.Close(f.fd)
	f.fd = -1
	return fs.ToErrno(err)
}

func (f *FileHandle) Flush(ctx context.Context) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	newFd, err := syscall.Dup(f.fd)
	if err != nil {
		return fs.ToErrno(err)
	}
	return fs.ToErrno(syscall.Close(newFd))
}

func (f *FileHandle) Fsync(ctx context.Context, flags uint32) (errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return fs.ToErrno(syscall.Fsync(f.fd))
}

func (f *FileHandle) Getlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, out *fuse.FileLock) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	var flk syscall.Flock_t
	lk.ToFlockT(&flk)
	errno := fs.ToErrno(syscall.FcntlFlock(uintptr(f.fd), F_OFD_GETLK, &flk))
	out.FromFlockT(&flk)
	return errno
}

func (f *FileHandle) Setlk(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setLock(ctx, owner, lk, flags, false)
}

func (f *FileHandle) Setlkw(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32) (errno syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.setLock(ctx, owner, lk, flags, true)
}

func (f *FileHandle) setLock(ctx context.Context, owner uint64, lk *fuse.FileLock, flags uint32, blocking bool) (errno syscall.Errno) {
	// SQLite doesn't seem to use this code path so we're returning "not supported".
	if (flags & fuse.FUSE_LK_FLOCK) != 0 {
		return syscall.Errno(fuse.ENOTSUP)
	}

	var flk syscall.Flock_t
	lk.ToFlockT(&flk)

	var op int
	if blocking {
		op = F_OFD_SETLKW
	} else {
		op = F_OFD_SETLK
	}

	if err := syscall.FcntlFlock(uintptr(f.fd), op, &flk); err != nil {
		return fs.ToErrno(err)
	}
	return fs.OK
}

/*
func (f *FileHandle) Setattr(ctx context.Context, in *fuse.SetAttrIn, out *fuse.AttrOut) syscall.Errno {
	if errno := f.setAttr(ctx, in); errno != 0 {
		return errno
	}
	return f.Getattr(ctx, out)
}

func (f *FileHandle) setAttr(ctx context.Context, in *fuse.SetAttrIn) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	var errno syscall.Errno
	if mode, ok := in.GetMode(); ok {
		if errno = fs.ToErrno(syscall.Fchmod(f.fd, mode)); errno != 0 {
			return errno
		}
	}

	uid32, uOk := in.GetUID()
	gid32, gOk := in.GetGID()
	if uOk || gOk {
		uid := -1
		gid := -1

		if uOk {
			uid = int(uid32)
		}
		if gOk {
			gid = int(gid32)
		}
		errno = fs.ToErrno(syscall.Fchown(f.fd, uid, gid))
		if errno != 0 {
			return errno
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
		errno = f.utimens(ap, mp)
		if errno != 0 {
			return errno
		}
	}

	if sz, ok := in.GetSize(); ok {
		errno = fs.ToErrno(syscall.Ftruncate(f.fd, int64(sz)))
		if errno != 0 {
			return errno
		}
	}
	return OK
}
*/

func (f *FileHandle) Getattr(ctx context.Context, a *fuse.AttrOut) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()

	var st syscall.Stat_t
	if err := syscall.Fstat(f.fd, &st); err != nil {
		return fs.ToErrno(err)
	}
	a.FromStat(&st)

	return fs.OK
}

func (f *FileHandle) Lseek(ctx context.Context, off uint64, whence uint32) (uint64, syscall.Errno) {
	f.mu.Lock()
	defer f.mu.Unlock()
	n, err := unix.Seek(f.fd, int64(off), int(whence))
	return uint64(n), fs.ToErrno(err)
}

func (f *FileHandle) Allocate(ctx context.Context, off uint64, sz uint64, mode uint32) syscall.Errno {
	f.mu.Lock()
	defer f.mu.Unlock()
	if err := syscall.Fallocate(f.fd, mode, int64(off), int64(sz)); err != nil {
		return fs.ToErrno(err)
	}
	return fs.OK
}

func readTxBuffer(filename string, offset, size int64) ([]byte, error) {
	f, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	if _, err := f.Seek(offset, io.SeekStart); err != nil {
		return nil, err
	}

	pageN := int(size / (PageSize + WALFrameHeaderSize))

	input := make([]byte, PageSize+WALFrameHeaderSize)
	output := make([]byte, (pageN*(liteserver.PageSize+liteserver.PageHeaderSize))+liteserver.TxFileHeaderSize)
	pages := output[0:]
	pgnos := output[pageN*PageSize:]
	for i := 0; i < pageN; i++ {
		if _, err := io.ReadFull(f, input); err != nil {
			return nil, fmt.Errorf("read wal frame: %w", err)
		}
		pgno := binary.BigEndian.Uint32(input[0:])

		// Copy page data to output buffer.
		copy(pages, input[WALFrameHeaderSize:])
		pages = pages[PageSize:]

		// Write page number to end
		binary.BigEndian.PutUint32(pgnos, pgno)
		pgnos = pages[liteserver.PageHeaderSize:]
	}

	return output, nil
}
