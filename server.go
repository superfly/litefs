package litefs

import (
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
	_ "github.com/mattn/go-sqlite3"
)

type Server struct {
	mountPath string
	dev       uint64 // shadow device inode

	server   *fuse.Server
	fs       fuse.RawFileSystem // root file system
	rootNode *RootNode

	URL   string
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

// ShadowPath returns the path that stores the data for the filesystem.
func (s *Server) ShadowPath() string {
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
	// opts.EnableLocks = true

	// Create shadow path next to directory.
	if err := os.MkdirAll(s.ShadowPath(), 0755); err != nil {
		return fmt.Errorf("mkdir shadow: %w", err)
	}

	var st syscall.Stat_t
	if err := syscall.Stat(s.ShadowPath(), &st); err != nil {
		return fmt.Errorf("stat shadow: %w", err)
	}

	// TEMP: Initialize empty "db" with journal mode set.
	if err := s.initDB(); err != nil {
		return fmt.Errorf("init db: %w", err)
	}

	s.rootNode = NewRootNode(uint64(st.Dev), s.ShadowPath())
	s.rootNode.URL = s.URL

	s.fs = fs.NewNodeFS(s.rootNode, opts)
	if s.server, err = fuse.NewServer(s.fs, s.mountPath, &opts.MountOptions); err != nil {
		return fmt.Errorf("new fuse server: %w", err)
	}

	// TEMP: Support multiple dbs
	go s.rootNode.DB("db").monitor()

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

func (s *Server) initDB() error {
	filename := filepath.Join(s.ShadowPath(), "db")

	if _, err := os.Stat(filename); err == nil {
		log.Printf("db already exists, skipping")
		return nil // db exists, skip
	} else if err != nil && !os.IsNotExist(err) {
		return err
	}

	db, err := sql.Open("sqlite3", filename)
	if err != nil {
		return err
	}
	defer db.Close()

	if _, err := db.Exec(`PRAGMA journal_mode = wal`); err != nil {
		return fmt.Errorf("set journal mode: %w", err)
	}

	return nil
}
