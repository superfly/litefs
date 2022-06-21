package litefs_test

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/superfly/litefs"
)

var debug = flag.Bool("debug", false, "enable fuse debugging")

func init() {
	log.SetFlags(0)
}

func TestFileSystem_CreateDB(t *testing.T) {
	fs := newOpenFileSystem(t)
	db := openSQLDB(t, filepath.Join(fs.Path(), "db"))

	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	var x int
	if err := db.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Close & reopen.
	db.Close()

	t.Log("reopening database...")

	db = openSQLDB(t, filepath.Join(fs.Path(), "db"))
	if _, err := db.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}
}

func newFileSystem(tb testing.TB) *litefs.FileSystem {
	tb.Helper()

	path := tb.TempDir()
	store := litefs.NewStore(filepath.Join(path, "data"))
	fs := litefs.NewFileSystem(filepath.Join(path, "mnt"), store)
	if err := os.MkdirAll(fs.Path(), 0777); err != nil {
		tb.Fatalf("cannot create mount point: %s", err)
	}
	fs.Debug = *debug

	return fs
}

func newOpenFileSystem(tb testing.TB) *litefs.FileSystem {
	tb.Helper()

	fs := newFileSystem(tb)
	if err := fs.Mount(); err != nil {
		tb.Fatalf("cannot open file system: %s", err)
	}

	tb.Cleanup(func() {
		if err := fs.Unmount(); err != nil {
			tb.Errorf("server close failed: %s", err)
		} else if err := os.RemoveAll(fs.Path()); err != nil {
			tb.Errorf("mount path removal failed: %s", err)
		} else if err := os.RemoveAll(fs.Store().Path()); err != nil {
			tb.Errorf("data path removal failed: %s", err)
		}
	})

	return fs
}

// openSQLDB opens a connection to a SQLite database.
func openSQLDB(tb testing.TB, dsn string) *sql.DB {
	tb.Helper()
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := db.Close(); err != nil {
			tb.Fatal(err)
		}
	})

	return db
}

/*
func Umount(tb testing.TB, path string) {
	tb.Helper()
	if buf, err := exec.Command("umount", "-f", path).CombinedOutput(); err != nil {
		tb.Errorf("umount failed: %s: %s", err, buf)
	}
}
*/
