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

func (db *DB) TXIDPath() string {
	return db.Path() + "-txid"
}

func (db *DB) TXID() (uint64, error) {
	buf, err := os.ReadFile(db.TXIDPath())
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(buf), 16, 64)
}

func (db *DB) SetTXID(txid uint64) error {
	// TODO: Atomic write
	return os.WriteFile(db.TXIDPath(), []byte(ltx.FormatTXID(txid)), 0644)
}

func (db *DB) Path() string {
	return filepath.Join(db.root.Path, db.name)
}

// TrimName removes "-shm" or "-wal" from the given name.
func TrimName(name string) string {
	name = strings.TrimSuffix(name, "-shm")
	return strings.TrimSuffix(name, "-wal")
}
