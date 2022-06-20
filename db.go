package litefs

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	// "github.com/superfly/ltx"
)

// DB represents a SQLite database.
type DB struct {
	id   uint64
	name string
	path string
	pos  Pos
}

// NewDB returns a new instance of DB.
func NewDB(id uint64, path string) *DB {
	return &DB{
		id:   id,
		path: path,
	}
}

// ID returns the database ID.
func (db *DB) ID() uint64 { return db.id }

// Name of the database name.
func (db *DB) Name() string { return db.name }

// Path of the database's data directory.
func (db *DB) Path() string { return db.path }

// Open initializes the database from files in its data directory.
func (db *DB) Open() error {
	// Read name file.
	name, err := os.ReadFile(filepath.Join(db.path, "name"))
	if err != nil {
		return fmt.Errorf("cannot find name file: %w", err)
	}
	db.name = string(name)

	return nil
}

// CreateJournal creates a new journal file on disk.
func (db *DB) CreateJournal() (*os.File, error) {
	return os.OpenFile(filepath.Join(db.path, FileTypeJournal.filename()), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
}

// WriteJournalLTX copies the current transaction to a new LTX file.
func (db *DB) WriteJournalLTX(ctx context.Context) error {
	// TODO: Read page numbers from journal file.
	// TODO: Sort page numbers.
	// TODO: Write LTX file to temporary location with next TXID.

	// TODO: Atomically rename LTX file to final location.
	// NOTE: Should this occur after the journal delete has succeeded?

	// TODO: Update DB's TXID

	return nil
}

// TrimName removes "-journal", "-shm" or "-wal" from the given name.
func TrimName(name string) string {
	if suffix := "-journal"; strings.HasSuffix(name, suffix) {
		name = strings.TrimSuffix(name, suffix)
	}
	if suffix := "-wal"; strings.HasSuffix(name, suffix) {
		name = strings.TrimSuffix(name, suffix)
	}
	if suffix := "-shm"; strings.HasSuffix(name, suffix) {
		name = strings.TrimSuffix(name, suffix)
	}
	return name
}
