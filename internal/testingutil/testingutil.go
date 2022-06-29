package testingutil

import (
	"database/sql"
	"testing"

	_ "github.com/mattn/go-sqlite3"
)

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

// ReopenSQLDB closes the existing database connection and reopens it with the DSN.
func ReopenSQLDB(tb testing.TB, db **sql.DB, dsn string) {
	tb.Helper()

	if err := (*db).Close(); err != nil {
		tb.Fatal(err)
	}
	*db = OpenSQLDB(tb, dsn)
}
