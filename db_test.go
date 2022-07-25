package litefs_test

import (
	// "path/filepath"
	"os"
	// "reflect"
	"testing"

	"github.com/superfly/litefs"
	// "github.com/superfly/litefs/internal/testingutil"
)

/*
// Ensure store can create a new, empty database.
func TestDB(t *testing.T) {
	db, _ := newDB(t, "test.db")
	// TODO: Reproduce initial database transaction as done by FUSE
}
*/

// newDB returns a new instance of DB attached to a temporary store.
func newDB(tb testing.TB, name string) (*litefs.DB, *os.File) {
	tb.Helper()

	store := newOpenStore(tb)
	db, f, err := store.CreateDB(name)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = f.Close() })

	return db, f
}
