package litefs

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type Server struct {
	mountPath string

	server *fuse.Server       // fuse server
	fs     fuse.RawFileSystem // root file system
	root   *Root              // root fuse node

	// If true, enables FUSE debug logging.
	// This can be excessive so you probably don't want to enable it.
	Debug bool
}

func NewServer(mountPath string) *Server {
	return &Server{
		mountPath: mountPath,
	}
}

// MountPath returns the path the server is being mounted to.
func (s *Server) MountPath() string {
	return s.mountPath
}

// DataPath returns the path that stores the data for the filesystem.
func (s *Server) DataPath() string {
	dir, base := filepath.Split(s.mountPath)
	return filepath.Join(dir, "."+base)
}

func (s *Server) Open() (err error) {
	sec := time.Second
	opts := &fs.Options{
		AttrTimeout:  &sec,
		EntryTimeout: &sec,
	}
	opts.Debug = s.Debug
	opts.MountOptions.Options = append(opts.MountOptions.Options, "fsname="+s.DataPath())
	opts.MountOptions.Name = "litefs"
	opts.NullPermissions = true
	// opts.EnableLocks = true

	if err := os.MkdirAll(s.DataPath(), 0755); err != nil {
		return fmt.Errorf("mkdir shadow: %w", err)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(s.DataPath(), &st); err != nil {
		return err
	}

	s.root = NewRoot(s.DataPath(), uint64(st.Dev))
	fileSystem := fs.NewNodeFS(s.root.NewNode(s.root.LoopbackRoot, nil, "", &st), opts)

	if s.server, err = fuse.NewServer(fileSystem, s.MountPath(), &opts.MountOptions); err != nil {
		return fmt.Errorf("new fuse server: %w", err)
	}

	go s.server.Serve()

	if err := s.server.WaitMount(); err != nil {
		return fmt.Errorf("wait mount: %w", err)
	}

	return nil
}

func (s *Server) Close() (err error) {
	if s.server != nil {
		if e := s.server.Unmount(); err == nil {
			err = e
		}
	}
	return err
}

func (s *Server) Wait() {
	s.server.Wait()
}
