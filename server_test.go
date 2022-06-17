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

var (
	debug = flag.Bool("debug", false, "enable fuse debugging")
)

func init() {
	log.SetFlags(0)
}

// Ensures a basic sanity check for mounting/unmounting a FUSE server.
func TestServer_Open(t *testing.T) {
	s := NewServer(t)
	if err := s.Open(); err != nil {
		t.Fatal(err)
	} else if err := s.Close(); err != nil {
		t.Fatal(err)
	}
}

// Ensure SQLite can create a database on a LiteFS mount.
func TestServer_CreateDB_Journal(t *testing.T) {
	s := NewOpenServer(t)
	db := OpenSQLDB(t, filepath.Join(s.MountPath(), "db"))

	println("dbg/create.table>>>>>")
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	println("dbg/insert>>>>>")
	if _, err := db.Exec(`INSERT INTO t (x) VALUES (1)`); err != nil {
		t.Fatal(err)
	}
	println("dbg/done>>>>>")
}

func NewServer(tb testing.TB) *litefs.Server {
	tb.Helper()

	path := tb.TempDir()
	s := litefs.NewServer(path)
	s.Debug = *debug

	tb.Cleanup(func() {
		if err := s.Close(); err != nil {
			tb.Errorf("server close failed: %s", err)
		}

		if err := os.RemoveAll(s.MountPath()); err != nil {
			tb.Errorf("mount path removal failed: %s", err)
		} else if err := os.RemoveAll(s.DataPath()); err != nil {
			tb.Errorf("data path removal failed: %s", err)
		}
	})

	return s
}

func NewOpenServer(tb testing.TB) *litefs.Server {
	tb.Helper()

	s := NewServer(tb)
	if err := s.Open(); err != nil {
		tb.Fatalf("cannot open server: %s", err)
	}

	tb.Cleanup(func() {
		if err := s.Close(); err != nil {
			tb.Errorf("server close failed: %s", err)
		} else if err := os.RemoveAll(s.DataPath()); err != nil {
			tb.Errorf("data path removal failed: %s", err)
		}
	})

	return s
}

// OpenSQLDB opens a connection to a SQLite database.
func OpenSQLDB(tb testing.TB, dsn string) *sql.DB {
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
