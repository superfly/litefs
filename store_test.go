package litefs_test

import (
	"path/filepath"
	"reflect"
	"testing"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal/testingutil"
)

// Ensure store can create a new, empty database.
func TestStore_CreateDB(t *testing.T) {
	store := newOpenStore(t)

	// Database should be empty.
	db, f, err := store.CreateDB("test1.db")
	if err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if got, want := db.ID(), uint32(1); got != want {
		t.Fatalf("ID=%v, want %v", got, want)
	}
	if got, want := db.Name(), "test1.db"; got != want {
		t.Fatalf("Name=%s, want %s", got, want)
	}
	if got, want := db.Pos(), (litefs.Pos{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("Pos=%#v, want %#v", got, want)
	}
	if got, want := db.TXID(), uint64(0); !reflect.DeepEqual(got, want) {
		t.Fatalf("TXID=%#v, want %#v", got, want)
	}
	if got, want := db.Path(), filepath.Join(store.Path(), "00000001"); got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
	if got, want := db.LTXDir(), filepath.Join(store.Path(), "00000001", "ltx"); got != want {
		t.Fatalf("LTXDir=%s, want %s", got, want)
	}
	if got, want := db.LTXPath(1, 2), filepath.Join(store.Path(), "00000001", "ltx", "0000000000000001-0000000000000002.ltx"); got != want {
		t.Fatalf("LTXPath=%s, want %s", got, want)
	}

	// Ensure next database uses the next highest identifier.
	db, f, err = store.CreateDB("test2.db")
	if err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	if got, want := db.ID(), uint32(2); got != want {
		t.Fatalf("ID=%v, want %v", got, want)
	}
}

func TestStore_Open(t *testing.T) {
	t.Run("ExistingEmptyDB", func(t *testing.T) {
		store := newStoreFromFixture(t, "testdata/store/open-name-only")
		if err := store.Open(); err != nil {
			t.Fatal(err)
		}

		db := store.DB(1)
		if db == nil {
			t.Fatal("expected database")
		}
		if got, want := db.ID(), uint32(1); got != want {
			t.Fatalf("id=%v, want %v", got, want)
		}
		if got, want := db.Name(), "test.db"; got != want {
			t.Fatalf("name=%v, want %v", got, want)
		}
		if got, want := db.Pos(), (litefs.Pos{}); got != want {
			t.Fatalf("pos=%v, want %v", got, want)
		}

		// Ensure next database uses the next highest identifier.
		db, f, err := store.CreateDB("test2.db")
		if err != nil {
			t.Fatal(err)
		} else if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, want := db.ID(), uint32(2); got != want {
			t.Fatalf("ID=%v, want %v", got, want)
		}
	})
}

// newStore returns a new instance of a Store on a temporary directory.
// This store will automatically close when the test ends.
func newStore(tb testing.TB) *litefs.Store {
	store := litefs.NewStore(tb.TempDir(), true)
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Fatalf("cannot close store: %s", err)
		}
	})
	return store
}

// newOpenStore returns a new instance of an empty, opened store.
func newOpenStore(tb testing.TB) *litefs.Store {
	tb.Helper()
	store := newStore(tb)
	if err := store.Open(); err != nil {
		tb.Fatal(err)
	}
	return store
}

func newStoreFromFixture(tb testing.TB, path string) *litefs.Store {
	tb.Helper()
	store := newStore(tb)
	testingutil.MustCopyDir(tb, path, store.Path())
	return store
}
