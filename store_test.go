package litefs_test

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"path/filepath"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal/testingutil"
	"github.com/superfly/litefs/mock"
	"github.com/superfly/ltx"
)

// Ensure store can create a new, empty database.
func TestStore_CreateDB(t *testing.T) {
	store := newOpenStore(t, newPrimaryStaticLeaser(), nil)

	// Database should be empty.
	db, f, err := store.CreateDB("test1.db")
	if err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	if got, want := db.Name(), "test1.db"; got != want {
		t.Fatalf("Name=%s, want %s", got, want)
	}
	if got, want := db.Pos(), (ltx.Pos{}); !reflect.DeepEqual(got, want) {
		t.Fatalf("Pos=%#v, want %#v", got, want)
	}
	if got, want := db.TXID(), ltx.TXID(0); !reflect.DeepEqual(got, want) {
		t.Fatalf("TXID=%#v, want %#v", got, want)
	}
	if got, want := db.Path(), filepath.Join(store.Path(), "dbs", "test1.db"); got != want {
		t.Fatalf("Path=%s, want %s", got, want)
	}
	if got, want := db.LTXDir(), filepath.Join(store.Path(), "dbs", "test1.db", "ltx"); got != want {
		t.Fatalf("LTXDir=%s, want %s", got, want)
	}
	if got, want := db.LTXPath(1, 2), filepath.Join(store.Path(), "dbs", "test1.db", "ltx", "0000000000000001-0000000000000002.ltx"); got != want {
		t.Fatalf("LTXPath=%s, want %s", got, want)
	}

	// Ensure we can create another database.
	_, f, err = store.CreateDB("test2.db")
	if err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestStore_Open(t *testing.T) {
	t.Run("ExistingEmptyDB", func(t *testing.T) {
		store := newStoreFromFixture(t, newPrimaryStaticLeaser(), nil, "testdata/store/open-name-only")
		if err := store.Open(); err != nil {
			t.Fatal(err)
		}

		db := store.DB("test.db")
		if db == nil {
			t.Fatal("expected database")
		}
		if got, want := db.Name(), "test.db"; got != want {
			t.Fatalf("name=%v, want %v", got, want)
		}
		if got, want := db.Pos(), (ltx.Pos{}); got != want {
			t.Fatalf("pos=%v, want %v", got, want)
		}

		// Ensure next database uses the next highest identifier.
		db, f, err := store.CreateDB("test2.db")
		if err != nil {
			t.Fatal(err)
		} else if err := f.Close(); err != nil {
			t.Fatal(err)
		}
		if got, want := db.Name(), "test2.db"; got != want {
			t.Fatalf("Name=%v, want %v", got, want)
		}
	})

	t.Run("InvalidDatabaseHeader", func(t *testing.T) {
		store := newStoreFromFixture(t, newPrimaryStaticLeaser(), nil, "testdata/store/open-invalid-database-header")
		if err := store.Open(); err != nil {
			t.Fatal(err)
		}

		db := store.DB("test.db")
		if db == nil {
			t.Fatal("expected database")
		}
		if got, want := db.Name(), "test.db"; got != want {
			t.Fatalf("name=%v, want %v", got, want)
		}
		if got, want := db.Pos(), (ltx.Pos{}); got != want {
			t.Fatalf("pos=%v, want %v", got, want)
		}
	})

	t.Run("ShortDatabase", func(t *testing.T) {
		store := newStoreFromFixture(t, newPrimaryStaticLeaser(), nil, "testdata/store/open-short-database")
		if err := store.Open(); err != nil {
			t.Fatal(err)
		}

		db := store.DB("test.db")
		if db == nil {
			t.Fatal("expected database")
		}
		if got, want := db.Name(), "test.db"; got != want {
			t.Fatalf("name=%v, want %v", got, want)
		}
		if got, want := db.Pos(), (ltx.Pos{}); got != want {
			t.Fatalf("pos=%v, want %v", got, want)
		}
	})
}

// Ensures that an existing database can write a snapshot after open.
// See: https://github.com/superfly/litefs/issues/173
func TestStore_OpenAndWriteSnapshot(t *testing.T) {
	store := newStoreFromFixture(t, newPrimaryStaticLeaser(), nil, "testdata/store/open-and-write-snapshot")
	if err := store.Open(); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	db := store.DB("sqlite.db")
	if _, _, err := db.WriteSnapshotTo(context.Background(), &buf); err != nil {
		t.Fatal(err)
	}

	dec := ltx.NewDecoder(&buf)
	if err := dec.Verify(); err != nil {
		t.Fatal(err)
	}
}

func TestPrimaryInfo_Clone(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		info := &litefs.PrimaryInfo{Hostname: "foo", AdvertiseURL: "bar"}
		if other := info.Clone(); *other != *info {
			t.Fatal("mismatch")
		}
	})
	t.Run("Nil", func(t *testing.T) {
		var info *litefs.PrimaryInfo
		if other := info.Clone(); other != nil {
			t.Fatal("expected nil")
		}
	})
}

// Ensure store generates a unique ID.
func TestStore_ID(t *testing.T) {
	store := newStore(t, newPrimaryStaticLeaser(), nil)
	if err := store.Open(); err != nil {
		t.Fatal(err)
	} else if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	id := store.ID()
	if id == 0 {
		t.Fatal("expected id")
	}
}

// Ensure store returns a context that is done when node loses primary status.
func TestStore_PrimaryCtx(t *testing.T) {
	t.Run("InitialPrimary", func(t *testing.T) {
		store := newOpenStore(t, newPrimaryStaticLeaser(), nil)
		if ctx := store.PrimaryCtx(context.Background()); ctx.Err() != nil {
			t.Fatal("expected no error")
		}
	})

	t.Run("PrimaryLost", func(t *testing.T) {
		var isPrimary atomic.Bool
		isPrimary.Store(true)

		lease := mock.Lease{
			RenewedAtFunc: func() time.Time { return time.Time{} },
			TTLFunc:       func() time.Duration { return 10 * time.Millisecond },
			RenewFunc: func(ctx context.Context) error {
				if !isPrimary.Load() {
					return litefs.ErrLeaseExpired
				}
				return nil
			},
			HandoffChFunc: func() <-chan uint64 { return nil },
			CloseFunc:     func() error { return nil },
		}

		var clusterID string
		leaser := mock.Leaser{
			CloseFunc:        func() error { return nil },
			AdvertiseURLFunc: func() string { return "http://localhost:20202" },
			AcquireFunc: func(ctx context.Context) (litefs.Lease, error) {
				return &lease, nil
			},
			PrimaryInfoFunc: func(ctx context.Context) (litefs.PrimaryInfo, error) {
				return litefs.PrimaryInfo{}, litefs.ErrNoPrimary
			},
			ClusterIDFunc: func(ctx context.Context) (string, error) {
				return clusterID, nil
			},
			SetClusterIDFunc: func(ctx context.Context, id string) error {
				clusterID = id
				return nil
			},
		}

		client := mock.Client{
			StreamFunc: func(ctx context.Context, rawurl string, nodeID uint64, posMap map[string]ltx.Pos, filter []string) (litefs.Stream, error) {
				return &mock.Stream{
					ReadCloser:    io.NopCloser(&bytes.Buffer{}),
					ClusterIDFunc: func() string { return "" },
				}, nil
			},
		}

		store := newOpenStore(t, &leaser, &client)

		// Ensure store starts in primary state.
		ctx := store.PrimaryCtx(context.Background())
		if ctx.Err() != nil {
			t.Fatal("expected no error")
		}

		// Mark lease as unrenewable so that store loses lease.
		isPrimary.Store(false)

		// Check context until it closes.
		testingutil.RetryUntil(t, 1*time.Millisecond, 5*time.Second, func() error {
			select {
			case <-ctx.Done():
				return nil // ok
			default:
				return fmt.Errorf("expected closed context")
			}
		})
	})

	t.Run("InitialReplica", func(t *testing.T) {
		leaser := litefs.NewStaticLeaser(false, "localhost", "http://localhost:20202")
		client := mock.Client{
			StreamFunc: func(ctx context.Context, rawurl string, nodeID uint64, posMap map[string]ltx.Pos, filter []string) (litefs.Stream, error) {
				var buf bytes.Buffer
				if err := litefs.WriteStreamFrame(&buf, &litefs.ReadyStreamFrame{}); err != nil {
					return nil, err
				}
				return &mock.Stream{
					ReadCloser:    io.NopCloser(&buf),
					ClusterIDFunc: func() string { return "" },
				}, nil
			},
		}

		store := newOpenStore(t, leaser, &client)
		ctx := store.PrimaryCtx(context.Background())
		if err := ctx.Err(); err != litefs.ErrLeaseExpired {
			t.Fatalf("unexpected error: %#v", err)
		}
	})

	t.Run("ParentCancelation", func(t *testing.T) {
		store := newOpenStore(t, newPrimaryStaticLeaser(), nil)

		ctx, cancel := context.WithCancel(context.Background())
		if ctx := store.PrimaryCtx(ctx); ctx.Err() != nil {
			t.Fatal("expected no error")
		}

		// Cancel & wait for propagation.
		cancel()
		select {
		case <-time.After(5 * time.Second):
			t.Fatal("timeout waiting for parent cancelation")
		case <-ctx.Done():
			if err := ctx.Err(); err != context.Canceled {
				t.Fatalf("unexpected error: %s", err)
			}
		}
	})
}

// newStore returns a new instance of a Store on a temporary directory.
// This store will automatically close when the test ends.
func newStore(tb testing.TB, leaser litefs.Leaser, client litefs.Client) *litefs.Store {
	store := litefs.NewStore(tb.TempDir(), true)
	store.Leaser = leaser
	store.Client = client
	tb.Cleanup(func() {
		if err := store.Close(); err != nil {
			tb.Fatalf("cannot close store: %s", err)
		}
	})
	return store
}

// newOpenStore returns a new instance of an empty, opened store.
func newOpenStore(tb testing.TB, leaser litefs.Leaser, client litefs.Client) *litefs.Store {
	tb.Helper()
	store := newStore(tb, leaser, client)
	if err := store.Open(); err != nil {
		tb.Fatal(err)
	}

	select {
	case <-time.After(5 * time.Second):
		tb.Fatal("timeout waiting for store ready")
	case <-store.ReadyCh(): // wait for lease
	}
	return store
}

func newStoreFromFixture(tb testing.TB, leaser litefs.Leaser, client litefs.Client, path string) *litefs.Store {
	tb.Helper()
	store := newStore(tb, leaser, client)
	testingutil.MustCopyDir(tb, path, store.Path())
	return store
}
