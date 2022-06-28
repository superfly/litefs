package integration

import (
	"database/sql"
	"path/filepath"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

func TestSingleNode_OK(t *testing.T) {
	fs := newOpenFileSystem(t)
	dsn := filepath.Join(fs.Path(), "db")
	db := openSQLDB(t, dsn)

	// Create a simple table with a single value.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Ensure we can retrieve the data back from the database.
	var x int
	if err := db.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Close & reopen.
	reopenSQLDB(t, &db, dsn)
	db = openSQLDB(t, dsn)
	if _, err := db.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}
}

func TestSingleNode_Rollback(t *testing.T) {
	fs := newOpenFileSystem(t)
	dsn := filepath.Join(fs.Path(), "db")
	db := openSQLDB(t, dsn)

	// Create a simple table with a single value.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}

	// Attempt to insert data but roll it back.
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	} else if _, err := tx.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		tx.Rollback()
		t.Fatal(err)
	} else if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Ensure we can retrieve the data back from the database.
	var x int
	if err := db.QueryRow(`SELECT x FROM t`).Scan(&x); err != sql.ErrNoRows {
		t.Fatalf("expected no rows (%#v)", err)
	}
}
