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
	"github.com/superfly/litefs/fuse"
)

func main() {
	log.SetFlags(0)

	if err := run(context.Background()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) (err error) {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt)
	defer stop()

	debug := flag.Bool("debug", false, "print debug information")
	flag.Parse()

	// First argument is the mount point for the file system.
	mountDir := flag.Arg(0)
	if mountDir == "" {
		return fmt.Errorf("usage: litefs MOUNTPOINT")
	} else if mountDir, err = filepath.Abs(mountDir); err != nil {
		return fmt.Errorf("abs: %w", err)
	}

	// Create a store to manage internal data.
	dir, file := filepath.Split(mountDir)
	store := litefs.NewStore(filepath.Join(dir, "."+file))
	if err := store.Open(); err != nil {
		return fmt.Errorf("cannot open store: %w", err)
	}

	// Build the file system to interact with the store.
	fs := fuse.NewFileSystem(mountDir, store)
	fs.Debug = *debug
	if err := fs.Mount(); err != nil {
		return fmt.Errorf("cannot open file system: %s", err)
	}
	defer fs.Unmount()

	log.Printf("LiteFS mounted to: %s", mountDir)

	// Wait for signal before exiting.
	<-ctx.Done()
	fmt.Println("received CTRL-C, exiting")

	if err := fs.Unmount(); err != nil {
		return fmt.Errorf("cannot unmount: %w", err)
	}
	fmt.Println("unmount successful")

	return nil
}
