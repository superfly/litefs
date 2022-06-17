package litefs

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/superfly/ltx"
)

// DB represents a SQLite database.
type DB struct {
	root *Root
	name string
}

// NewDB returns a new instance of DB.
func NewDB(root *Root, name string) *DB {
	return &DB{
		root: root,
		name: name,
	}
}

// Path of the main database file on-disk.
func (db *DB) Path() string {
	return filepath.Join(db.root.Path, db.name)
}

// Path of the directory that holds the metadata for the database.
func (db *DB) MetaDir() string {
	return filepath.Join(db.root.Path, ".litefs", db.name)
}

// WriteJournalLTX copies the current transaction to a new LTX file.
func (db *DB) WriteJournalLTX(ctx context.Context) error {
	if err := os.Mkdir(db.MetaDir(), 0755); err != nil {
		return err
	}

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
