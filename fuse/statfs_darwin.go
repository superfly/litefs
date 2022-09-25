package fuse

import (
	"context"
	"syscall"

	"bazil.org/fuse"
)

// Statfs is a passthrough to the underlying file system.
func (fsys *FileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	// Obtain statfs() call from underlying store path.
	var statfs syscall.Statfs_t
	path := fsys.store.Path()
	if err := syscall.Statfs(path, &statfs); err != nil {
		return err
	}

	// Copy stats over from the underlying file system to the response.
	resp.Blocks = statfs.Blocks
	resp.Bfree = statfs.Bfree
	resp.Bavail = statfs.Bavail
	resp.Files = statfs.Files
	resp.Ffree = statfs.Ffree
	resp.Bsize = uint32(statfs.Bsize)
	// This should be available via pathconf(_, _PC_PATH_MAX), but isn't exposed nicely in Go.
	resp.Namelen = 1024
	// I can't find where this is exposed on MacOS. It should be 512, 1024 or 4096 (recent
	// MacBooks?). I'm guessing the hardware-level capability is intended, not the filesystem-level
	// choice. The block size seems to match for now.
	// https://stackoverflow.com/questions/61391135/get-block-file-size-on-macos-with-statfs
	resp.Frsize = statfs.Bsize

	return nil
}
