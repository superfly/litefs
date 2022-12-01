package main_test

import (
	"context"
	"path/filepath"
	"strings"
	"testing"

	main "github.com/superfly/litefs/cmd/litefs"
	"github.com/superfly/litefs/internal/testingutil"
)

// Ensure a new, fresh database can be imported to a LiteFS server.
func TestImportCommand_Create(t *testing.T) {
	// Generate a database on the regular file system.
	dsn := filepath.Join(t.TempDir(), "db")
	db := testingutil.OpenSQLDB(t, dsn)
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Run a LiteFS mount.
	m0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, m0)

	// Import database into LiteFS.
	cmd := main.NewImportCommand()
	cmd.URL = m0.HTTPServer.URL()
	cmd.Name = "my.db"
	cmd.Path = dsn
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read from LiteFS mount.
	db = testingutil.OpenSQLDB(t, filepath.Join(m0.Config.MountDir, "my.db"))
	var x int
	if err := db.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
}

// Ensure an existing database can be overwritten by an import.
func TestImportCommand_Overwrite(t *testing.T) {
	// Generate a database on the regular file system.
	dsn := filepath.Join(t.TempDir(), "db")
	dbx := testingutil.OpenSQLDB(t, dsn)
	if _, err := dbx.Exec(`CREATE TABLE u (y)`); err != nil {
		t.Fatal(err)
	} else if _, err := dbx.Exec(`INSERT INTO u VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := dbx.Close(); err != nil {
		t.Fatal(err)
	}

	// Run an LiteFS mount.
	m0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, m0)

	// Generate data into the mount.
	db := testingutil.OpenSQLDB(t, filepath.Join(m0.Config.MountDir, "db"))
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 100; i++ {
		if _, err := db.Exec(`INSERT INTO t VALUES (?)`, strings.Repeat("x", 256)); err != nil {
			t.Fatal(err)
		}
	}

	// Overwrite database on LiteFS.
	cmd := main.NewImportCommand()
	cmd.URL = m0.HTTPServer.URL()
	cmd.Name = "db"
	cmd.Path = dsn
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	// Read from LiteFS mount.
	var y int
	if err := db.QueryRow(`SELECT y FROM u`).Scan(&y); err != nil {
		t.Fatal(err)
	} else if got, want := y, 100; got != want {
		t.Fatalf("y=%d, want %d", got, want)
	}

	// Reconnect and verify correctness.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	db = testingutil.OpenSQLDB(t, filepath.Join(m0.Config.MountDir, "db"))
	if err := db.QueryRow(`SELECT y FROM u`).Scan(&y); err != nil {
		t.Fatal(err)
	} else if got, want := y, 100; got != want {
		t.Fatalf("y=%d, want %d", got, want)
	}
}
