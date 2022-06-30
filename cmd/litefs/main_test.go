package main_test

import (
	"context"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"testing"
	"time"

	main "github.com/superfly/litefs/cmd/litefs"
	"github.com/superfly/litefs/internal/testingutil"
)

var debug = flag.Bool("debug", false, "enable fuse debugging")

func init() {
	log.SetFlags(0)
	rand.Seed(time.Now().UnixNano())
}

func TestSingleNode(t *testing.T) {
	m0 := newRunningMain(t, t.TempDir(), nil)
	db := testingutil.OpenSQLDB(t, filepath.Join(m0.MountDir, "db"))

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
}

func TestMultiNode_Simple(t *testing.T) {
	m0 := newRunningMain(t, t.TempDir(), nil)
	waitForPrimary(t, m0)
	m1 := newRunningMain(t, t.TempDir(), m0)
	db0 := testingutil.OpenSQLDB(t, filepath.Join(m0.MountDir, "db"))
	db1 := testingutil.OpenSQLDB(t, filepath.Join(m1.MountDir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	var x int
	if err := db0.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Ensure we can retrieve the data back from the database on the second node.
	waitForSync(t, 1, m0, m1)
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
}

func TestMultiNode_ForcedReelection(t *testing.T) {
	m0 := newRunningMain(t, t.TempDir(), nil)
	waitForPrimary(t, m0)
	m1 := newRunningMain(t, t.TempDir(), m0)
	db0 := testingutil.OpenSQLDB(t, filepath.Join(m0.MountDir, "db"))
	db1 := testingutil.OpenSQLDB(t, filepath.Join(m1.MountDir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Wait for sync first.
	waitForSync(t, 1, m0, m1)
	var x int
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Stop the primary and wait for handoff to secondary.
	t.Log("shutting down primary node")
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	} else if err := m0.Close(); err != nil {
		t.Fatal(err)
	}

	// Second node should eventually become primary.
	t.Log("waiting for promotion of replica")
	waitForPrimary(t, m1)

	// Update record on the new primary.
	t.Log("updating record on new primary")
	if _, err := db1.Exec(`UPDATE t SET x = 200`); err != nil {
		t.Fatal(err)
	}

	// Reopen first node and ensure it propagates changes.
	t.Log("restarting first node as replica")
	m0 = newRunningMain(t, m0.MountDir, m1)
	waitForSync(t, 1, m0, m1)

	db0 = testingutil.OpenSQLDB(t, filepath.Join(m0.MountDir, "db"))

	t.Log("verifying propagation of record update")
	if err := db0.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
}

func newMain(tb testing.TB, mountDir string, peer *main.Main) *main.Main {
	tb.Helper()

	tb.Cleanup(func() {
		if err := os.RemoveAll(mountDir); err != nil {
			tb.Fatalf("cannot remove temp directory: %s", err)
		}
	})

	m := main.NewMain()
	m.MountDir = mountDir
	m.Addr = ":0"
	m.ConsulURL = "http://localhost:8500"
	m.ConsulTTL = 2 * time.Second
	m.ConsulLockDelay = 1 * time.Second
	m.Debug = *debug

	// Use peer's consul key, if passed in. Otherwise generate one.
	if peer != nil {
		m.ConsulKey = peer.ConsulKey
	} else {
		m.ConsulKey = fmt.Sprintf("%x", rand.Int31())
	}

	return m
}

func newRunningMain(tb testing.TB, mountDir string, peer *main.Main) *main.Main {
	tb.Helper()

	m := newMain(tb, mountDir, peer)
	if err := m.Run(context.Background()); err != nil {
		tb.Fatal(err)
	}

	tb.Cleanup(func() {
		if err := m.Close(); err != nil {
			log.Printf("cannot close main: %s", err)
		}
	})

	return m
}

// waitForPrimary waits for m to obtain the primary lease.
func waitForPrimary(tb testing.TB, m *main.Main) {
	tb.Helper()
	tb.Logf("waiting for primary...")
	testingutil.RetryUntil(tb, 1*time.Millisecond, 30*time.Second, func() error {
		if !m.Store.IsPrimary() {
			return fmt.Errorf("not primary")
		}
		return nil
	})
}

// waitForSync waits for all processes to sync to the same TXID.
func waitForSync(tb testing.TB, dbID uint64, mains ...*main.Main) {
	tb.Helper()
	testingutil.RetryUntil(tb, 1*time.Millisecond, 30*time.Second, func() error {
		db0 := mains[0].Store.FindDB(dbID)
		if db0 == nil {
			return fmt.Errorf("no database on main[0]")
		}

		txID := db0.TXID()
		for i, m := range mains {
			db := m.Store.FindDB(dbID)
			if db == nil {
				return fmt.Errorf("no database on main[%d]", i)
			}

			if got, want := db.TXID(), txID; got != want {
				return fmt.Errorf("waiting for sync on db(%d): [%d,%d]", dbID, got, want)
			}
		}

		tb.Logf("%d processes synced for db %d at tx %d", len(mains), dbID, txID)
		return nil
	})
}
