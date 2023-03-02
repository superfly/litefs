package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"time"

	"github.com/superfly/litefs/http"
	"github.com/superfly/litefs/internal"
)

// ExportCommand represents a command to export a database from the cluster.
type ExportCommand struct {
	// Target LiteFS URL
	URL string

	// Name of database on LiteFS cluster.
	Name string

	// Path to export the database to.
	Path string
}

// NewExportCommand returns a new instance of ExportCommand.
func NewExportCommand() *ExportCommand {
	return &ExportCommand{
		URL: DefaultURL,
	}
}

// ParseFlags parses the command line flags & config file.
func (c *ExportCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litefs-export", flag.ContinueOnError)
	fs.StringVar(&c.URL, "url", "http://localhost:20202", "LiteFS API URL")
	fs.StringVar(&c.Name, "name", "", "database name")
	fs.Usage = func() {
		fmt.Println(`
The export command will download a SQLite database from a LiteFS cluster. If the
database doesn't exist then an error will be returned.

Usage:

	litefs export [arguments] PATH

Arguments:
`[1:])
		fs.PrintDefaults()
		fmt.Println("")
	}
	if err := fs.Parse(args); err != nil {
		return err
	} else if fs.NArg() == 0 {
		fs.Usage()
		return flag.ErrHelp
	} else if fs.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Copy first arg as database path.
	c.Path = fs.Arg(0)

	return nil
}

// Run executes the command.
func (c *ExportCommand) Run(ctx context.Context) (err error) {
	// Clear existing database and related files.
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		if err := os.Remove(c.Path + suffix); err != nil && !os.IsNotExist(err) {
			return err
		}
	}

	tmpPath := c.Path + ".tmp"
	defer func() { _ = os.Remove(tmpPath) }()

	f, err := os.Create(tmpPath)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	t := time.Now()

	// Fetch snapshot from the server.
	client := http.NewClient()
	r, err := client.Export(ctx, c.URL, c.Name)
	if err != nil {
		return err
	}
	defer func() { _ = r.Close() }()

	// Copy bytes to temp file.
	if _, err := io.Copy(f, r); err != nil {
		return err
	} else if err := r.Close(); err != nil {
		return err
	}

	// Sync & close file.
	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	// Atomically rename & sync parent directory.
	if err := os.Rename(tmpPath, c.Path); err != nil {
		return err
	} else if err := internal.Sync(filepath.Dir(c.Path)); err != nil {
		return err
	}

	// Notify user of success and elapsed time.
	fmt.Printf("Export of database %q in %s\n", c.Name, time.Since(t))

	return nil
}
