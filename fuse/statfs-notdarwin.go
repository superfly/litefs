// This looks like it's unlikely to work on Windows but I've just fixed MacOS for now.

//go:build !darwin

package fuse

import (
	"syscall"

	"bazil.org/fuse"
)

// Statfs is a passthrough to the underlying file system.
func (fsys *FileSystem) Statfs(ctx context.Context, req *fuse.StatfsRequest, resp *fuse.StatfsResponse) error {
	// Obtain statfs() call from underlying store path.
	var statfs syscall.Statfs_t
	if err := syscall.Statfs(fsys.store.Path(), &statfs); err != nil {
		return err
	}

	// Copy stats over from the underlying file system to the response.
	resp.Blocks = statfs.Blocks
	resp.Bfree = statfs.Bfree
	resp.Bavail = statfs.Bavail
	resp.Files = statfs.Files
	resp.Ffree = statfs.Ffree
	resp.Bsize = uint32(statfs.Bsize)
	resp.Namelen = uint32(statfs.Namelen)
	resp.Frsize = uint32(statfs.Frsize)

	return nil
}
