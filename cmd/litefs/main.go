package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/consul"
	"github.com/superfly/litefs/fuse"
	"github.com/superfly/litefs/http"
)

func main() {
	log.SetFlags(0)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt)
	defer stop()

	m := NewMain()
	if err := m.ParseFlags(ctx, os.Args[1:]); err == flag.ErrHelp {
		os.Exit(2)
	} else if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}

	if err := m.Run(ctx); err != nil {
		fmt.Fprintln(os.Stderr, err)
		_ = m.Close()
		os.Exit(1)
	}

	// Wait for signal before exiting.
	<-ctx.Done()
	fmt.Println("received CTRL-C, exiting")

	if err := m.Close(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

// Main represents the command line program.
type Main struct {
	MountDir string

	Addr      string
	ConsulURL string

	Debug bool

	Store      *litefs.Store
	Leaser     *consul.Leaser
	FileSystem *fuse.FileSystem
	HTTPServer *http.Server
}

// NewMain returns a new instance of Main.
func NewMain() *Main {
	return &Main{}
}

// ParseFlags parses the command line flags.
func (m *Main) ParseFlags(ctx context.Context, args []string) error {
	fs := flag.NewFlagSet("litefs", flag.ContinueOnError)
	fs.BoolVar(&m.Debug, "debug", false, "print debug information")
	fs.StringVar(&m.Addr, "addr", ":20202", "http bind address")
	fs.StringVar(&m.ConsulURL, "consul-url", "", "")

	// TODO: Update usage: fmt.Errorf("usage: litefs MOUNTPOINT")

	// First argument is the mount point for the file system.
	if err := fs.Parse(args); err != nil {
		return err
	}

	m.MountDir = fs.Arg(0)
	return nil
}

func (m *Main) Close() (err error) {
	if m.HTTPServer != nil {
		if e := m.HTTPServer.Close(); err == nil {
			err = e
		}
	}

	if m.FileSystem != nil {
		if e := m.FileSystem.Unmount(); err == nil {
			err = e
		}
	}

	return err
}

func (m *Main) Run(ctx context.Context) (err error) {
	if m.MountDir == "" {
		return fmt.Errorf("required: mount path")
	} else if m.ConsulURL == "" {
		return fmt.Errorf("required: --consul-url URL")
	}

	// Start listening on HTTP server first so we can determine the URL.
	if err := m.initHTTPServer(ctx); err != nil {
		return fmt.Errorf("cannot init http server: %w", err)
	}

	if err := m.initConsul(ctx); err != nil {
		return fmt.Errorf("cannot init consul: %w", err)
	} else if err := m.initStore(ctx); err != nil {
		return fmt.Errorf("cannot init store: %w", err)
	}

	if err := m.initFileSystem(ctx); err != nil {
		return fmt.Errorf("cannot init file system: %w", err)
	}
	log.Printf("LiteFS mounted to: %s", m.FileSystem.Path())

	m.HTTPServer.Serve()
	log.Printf("http server listening on: %s", m.HTTPServer.URL())

	return nil
}

func (m *Main) initConsul(ctx context.Context) error {
	// TEMP: Allow non-localhost addresses.

	leaser := consul.NewLeaser(m.ConsulURL)
	leaser.AdvertiseURL = m.HTTPServer.URL()
	if err := leaser.Open(); err != nil {
		return fmt.Errorf("cannot connect to consul: %w", err)
	}

	m.Leaser = leaser
	return nil
}

func (m *Main) initStore(ctx context.Context) error {
	mountDir, err := filepath.Abs(m.MountDir)
	if err != nil {
		return fmt.Errorf("abs: %w", err)
	}
	dir, file := filepath.Split(mountDir)

	store := litefs.NewStore(filepath.Join(dir, "."+file))
	store.Client = http.NewClient()
	store.Leaser = m.Leaser
	if err := store.Open(); err != nil {
		return fmt.Errorf("cannot open store: %w", err)
	}

	m.Store = store
	return nil
}

func (m *Main) initFileSystem(ctx context.Context) error {
	mountDir, err := filepath.Abs(m.MountDir)
	if err != nil {
		return fmt.Errorf("abs: %w", err)
	}

	// Build the file system to interact with the store.
	fsys := fuse.NewFileSystem(mountDir, m.Store)
	fsys.Debug = m.Debug
	if err := fsys.Mount(); err != nil {
		return fmt.Errorf("cannot open file system: %s", err)
	}

	// Attach file system to store so it can invalidate the page cache.
	m.Store.InodeNotifier = fsys

	m.FileSystem = fsys
	return nil
}

func (m *Main) initHTTPServer(ctx context.Context) error {
	server := http.NewServer(m.Store, m.Addr)
	if err := server.Listen(); err != nil {
		return fmt.Errorf("cannot open http server: %w", err)
	}
	m.HTTPServer = server
	return nil
}
