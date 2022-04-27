package litefs

import (
	"context"
	"syscall"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type FileHandle struct {
	fsFileHandle
	node *Node
}

var (
	_ fs.FileHandle    = (*FileHandle)(nil)
	_ fs.FileReleaser  = (*FileHandle)(nil)
	_ fs.FileGetattrer = (*FileHandle)(nil)
	_ fs.FileReader    = (*FileHandle)(nil)
	_ fs.FileWriter    = (*FileHandle)(nil)
	_ fs.FileGetlker   = (*FileHandle)(nil)
	_ fs.FileSetlker   = (*FileHandle)(nil)
	_ fs.FileSetlkwer  = (*FileHandle)(nil)
	_ fs.FileLseeker   = (*FileHandle)(nil)
	_ fs.FileFlusher   = (*FileHandle)(nil)
	_ fs.FileFsyncer   = (*FileHandle)(nil)
	_ fs.FileSetattrer = (*FileHandle)(nil)
	_ fs.FileAllocater = (*FileHandle)(nil)
)

func (f *FileHandle) Read(ctx context.Context, buf []byte, off int64) (res fuse.ReadResult, errno syscall.Errno) {
	res, errno = f.fsFileHandle.Read(ctx, buf, off)
	return &readResult{ReadResult: res, f: f, off: off}, errno
}

func (f *FileHandle) Write(ctx context.Context, data []byte, off int64) (uint32, syscall.Errno) {
	written, errno := f.fsFileHandle.Write(ctx, data, off)
	return written, errno
}

type fsFileHandle interface {
	fs.FileHandle
	fs.FileReleaser
	fs.FileGetattrer
	fs.FileReader
	fs.FileWriter
	fs.FileGetlker
	fs.FileSetlker
	fs.FileSetlkwer
	fs.FileLseeker
	fs.FileFlusher
	fs.FileFsyncer
	fs.FileSetattrer
	fs.FileAllocater
}

type readResult struct {
	fuse.ReadResult
	f   *FileHandle
	off int64
}

func (rr *readResult) Bytes(buf []byte) ([]byte, fuse.Status) {
	ret, status := rr.ReadResult.Bytes(buf)
	return ret, status
}
