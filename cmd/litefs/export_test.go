// go:build linux
package main_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/superfly/litefs"
	main "github.com/superfly/litefs/cmd/litefs"
	"github.com/superfly/litefs/internal/testingutil"
)

// Ensure a database can be exported from a LiteFS server.
func TestExportCommand(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		m0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
		waitForPrimary(t, m0)

		// Create database on LiteFS.
		db := testingutil.OpenSQLDB(t, filepath.Join(m0.Config.FUSE.Dir, "my.db"))
		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db.Close(); err != nil {
			t.Fatal(err)
		}

		// Export database from LiteFS.
		dsn := filepath.Join(t.TempDir(), "db")
		cmd := main.NewExportCommand()
		cmd.URL = m0.HTTPServer.URL()
		cmd.Name = "my.db"
		cmd.Path = dsn
		if err := cmd.Run(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Read from exported database.
		db = testingutil.OpenSQLDB(t, dsn)
		var x int
		if err := db.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 100; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}
	})

	t.Run("ErrDatabaseNotFound", func(t *testing.T) {
		m0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
		waitForPrimary(t, m0)

		// Export missing database from LiteFS.
		cmd := main.NewExportCommand()
		cmd.URL = m0.HTTPServer.URL()
		cmd.Name = "nosuchdatabase"
		cmd.Path = filepath.Join(t.TempDir(), "db")
		if err := cmd.Run(context.Background()); err == nil || err != litefs.ErrDatabaseNotFound {
			t.Fatalf("unexpected error: %v", err)
		}
	})
}
