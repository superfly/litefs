package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/superfly/litefs/http"
)

// ImportCommand represents a command to import an existing SQLite database into a cluster.
type ImportCommand struct {
	// Target LiteFS URL
	URL string

	// Name of database on LiteFS cluster.
	Name string

	// SQLite database path to be imported
	Path string
}

// NewImportCommand returns a new instance of ImportCommand.
func NewImportCommand() *ImportCommand {
	return &ImportCommand{
		URL: DefaultURL,
	}
}

// ParseFlags parses the command line flags & config file.
func (c *ImportCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litefs-import", flag.ContinueOnError)
	fs.StringVar(&c.URL, "url", "http://localhost:20202", "LiteFS API URL")
	fs.StringVar(&c.Name, "name", "", "database name")
	fs.Usage = func() {
		fmt.Println(`
The import command will upload a SQLite database to a LiteFS cluster. If the
named database doesn't exist, it will be created. If it does exist, it will be
replaced. This command is safe to used on a live database.

The database file is not validated for integrity by LiteFS. You can perform an
integrity check first by running "PRAGMA integrity_check" from the SQLite CLI.

Usage:

	litefs import [arguments] PATH

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
func (c *ImportCommand) Run(ctx context.Context) (err error) {
	f, err := os.Open(c.Path)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	t := time.Now()

	client := http.NewClient()
	if err := client.Import(ctx, c.URL, c.Name, f); err != nil {
		return err
	}

	// Notify user of success and elapsed time.
	fmt.Printf("Import of database %q in %s\n", c.Name, time.Since(t))

	return nil
}
