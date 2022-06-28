package integration

import (
	"database/sql"
	"flag"
	"log"
	"os"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/superfly/litefs"
	"github.com/superfly/litefs/fuse"
)

var debug = flag.Bool("debug", false, "enable fuse debugging")

func init() {
	log.SetFlags(0)
}

func newFileSystem(tb testing.TB) *fuse.FileSystem {
	tb.Helper()

	path := tb.TempDir()
	store := litefs.NewStore(filepath.Join(path, "data"))
	fs := fuse.NewFileSystem(filepath.Join(path, "mnt"), store)
	if err := os.MkdirAll(fs.Path(), 0777); err != nil {
		tb.Fatalf("cannot create mount point: %s", err)
	}
	fs.Debug = *debug

	return fs
}

func newOpenFileSystem(tb testing.TB) *fuse.FileSystem {
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

// reopenSQLDB closes the existing database connection and reopens it with the DSN.
func reopenSQLDB(tb testing.TB, db **sql.DB, dsn string) {
	tb.Helper()
	if err := (*db).Close(); err != nil {
		tb.Fatal(err)
	}
	*db = openSQLDB(tb, dsn)
}

/*
func Umount(tb testing.TB, path string) {
	tb.Helper()
	if buf, err := exec.Command("umount", "-f", path).CombinedOutput(); err != nil {
		tb.Errorf("umount failed: %s: %s", err, buf)
	}
}
*/
