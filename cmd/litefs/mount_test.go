// go:build linux
package main_test

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	main "github.com/superfly/litefs/cmd/litefs"
	"github.com/superfly/litefs/internal/testingutil"
	"golang.org/x/sync/errgroup"
)

func TestSingleNode_OK(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

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

// Ensure that node does not open if there is file corruption.
func TestSingleNode_CorruptLTX(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Build some LTX files.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Determine TXID based on journal mode.
	txID := uint64(2)
	if testingutil.IsWALMode() {
		txID++
	}

	// Corrupt one of the LTX files.
	if f, err := os.OpenFile(cmd0.Store.DB("db").LTXPath(txID, txID), os.O_RDWR, 0666); err != nil {
		t.Fatal(err)
	} else if _, err := f.WriteAt([]byte("\xff\xff\xff\xff"), 96); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen process and verification should fail.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	}
	if err := cmd0.Run(context.Background()); err == nil || err.Error() != `cannot open store: open databases: open database("db"): sync wal to ltx: validate ltx: close reader: file checksum mismatch` {
		t.Fatalf("unexpected error: %s", err)
	}
}

// Ensure that node replays the last LTX file to fix the simulated corruption.
func TestSingleNode_RecoverFromLastLTX(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Build some LTX files.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the database file & close.
	if f, err := os.OpenFile(cmd0.Store.DB("db").DatabasePath(), os.O_RDWR, 0666); err != nil {
		t.Fatal(err)
	} else if _, err := f.WriteAt([]byte("\xff\xff\xff\xff"), 4096+200); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen process. Replayed LTX file should overrwrite corruption.
	if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd0.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// Ensure that node does not open if the database checksum does not match LTX.
func TestSingleNode_DatabaseChecksumMismatch(t *testing.T) {
	// This test case only works on 4KB pages because the checksums are
	// hard-coded into the tested error messages. This lets us detect if
	// checksumming has changed.
	pageSize := testingutil.PageSize()
	if pageSize != 4096 {
		t.Skip("non-standard page size, skipping")
	}

	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Build some LTX files.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`PRAGMA user_version = 1234`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Corrupt the database file on a page that was not in the last commit.
	if f, err := os.OpenFile(cmd0.Store.DB("db").DatabasePath(), os.O_RDWR, 0666); err != nil {
		t.Fatal(err)
	} else if _, err := f.WriteAt([]byte("\xff\xff\xff\xff"), 4096+200); err != nil {
		t.Fatal(err)
	} else if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen process and verification should fail.
	if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	}

	switch mode := testingutil.JournalMode(); mode {
	case "delete", "persist", "truncate":
		if err := cmd0.Run(context.Background()); err == nil || err.Error() != `cannot open store: open databases: open database("db"): recover ltx: database checksum 9d81a60d39fb4760 does not match LTX post-apply checksum ce5a5d55e91b3cd1` {
			t.Fatalf("unexpected error: %s", err)
		}
	case "wal":
		if err := cmd0.Run(context.Background()); err == nil || err.Error() != `cannot open store: open databases: open database("db"): recover ltx: database checksum a9e884061ea4e488 does not match LTX post-apply checksum fa337f5ece449f39` {
			t.Fatalf("unexpected error: %s", err)
		}
	default:
		t.Fatalf("invalid journal mode: %q", mode)
	}
}

// Ensure that node can recover if the initial database creation rolls back.
func TestSingleNode_RecoverFromInitialRollback(t *testing.T) {
	dir := t.TempDir()
	cmd := runMountCommand(t, newMountCommand(t, dir, nil))

	db, err := sql.Open("sqlite3", filepath.Join(cmd.Config.FUSE.Dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback() }()

	// Start creating the database and then rollback.
	if _, err := tx.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if err := tx.Rollback(); err != nil {
		t.Fatal(err)
	}

	// Close database & mount.
	if err := db.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen process. Should be able to start up and recreate the database.
	cmd = newMountCommand(t, dir, nil)
	if err := cmd.Run(context.Background()); err != nil {
		t.Fatal(err)
	}

	db, err = sql.Open("sqlite3", filepath.Join(cmd.Config.FUSE.Dir, "db"))
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	if err := cmd.Close(); err != nil {
		t.Fatal(err)
	}
}

func TestMultiNode_Simple(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

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
	waitForSync(t, "db", cmd0, cmd1)
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Write another value.
	if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Ensure it invalidates the page on the secondary.
	waitForSync(t, "db", cmd0, cmd1)
	if err := db1.QueryRow(`SELECT MAX(x) FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("count=%d, want %d", got, want)
	}
}

func TestMultiNode_LateJoinWithSnapshot(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Remove most LTX files through retention.
	if err := cmd0.Store.DB("db").EnforceRetention(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}

	var x int
	if err := db0.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Ensure we can retrieve the data back from the database on the second node.
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	waitForSync(t, "db", cmd0, cmd1)
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Write another value.
	if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Ensure it invalidates the page on the secondary.
	waitForSync(t, "db", cmd0, cmd1)
	if err := db1.QueryRow(`SELECT MAX(x) FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("count=%d, want %d", got, want)
	}
}

func TestMultiNode_RejoinWithSnapshot(t *testing.T) {
	dir0, dir1 := t.TempDir(), t.TempDir()
	cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, dir1, cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	// Shutdown replica.
	if err := cmd1.Close(); err != nil {
		t.Fatal(err)
	}

	// Issue more transactions & remove other LTX files.
	if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (300)`); err != nil {
		t.Fatal(err)
	} else if err := cmd0.Store.DB("db").EnforceRetention(context.Background(), time.Now()); err != nil {
		t.Fatal(err)
	}

	// Reopen replica. It should require a snapshot and then remove other LTX files.
	cmd1 = runMountCommand(t, newMountCommand(t, dir1, cmd0))
	waitForSync(t, "db", cmd0, cmd1)

	// Verify data is correct on replica
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	var n int
	if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&n); err != nil {
		t.Fatal(err)
	} else if got, want := n, 600; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Ensure only the snapshot LTX file exists.
	if ents, err := cmd1.Store.DB("db").ReadLTXDir(); err != nil {
		t.Fatal(err)
	} else if got, want := len(ents), 1; got != want {
		t.Fatalf("len(entries)=%d, want %d", got, want)
	} else {
		if testingutil.IsWALMode() {
			if got, want := ents[0].Name(), `0000000000000001-0000000000000005.ltx`; got != want {
				t.Fatalf("enties[0].Name()=%s, want %s", got, want)
			}
		} else {
			if got, want := ents[0].Name(), `0000000000000001-0000000000000004.ltx`; got != want {
				t.Fatalf("enties[0].Name()=%s, want %s", got, want)
			}
		}
	}
}

func TestMultiNode_NonStandardPageSize(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	if _, err := db0.Exec(`PRAGMA page_size = 512`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
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
	waitForSync(t, "db", cmd0, cmd1)
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}

	// Write another value.
	if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Ensure it invalidates the page on the secondary.
	waitForSync(t, "db", cmd0, cmd1)
	if err := db1.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 2; got != want {
		t.Fatalf("count=%d, want %d", got, want)
	}
}

func TestMultiNode_ForcedReelection(t *testing.T) {
	dir0, dir1 := t.TempDir(), t.TempDir()
	cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, dir1, cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Wait for sync first.
	waitForSync(t, "db", cmd0, cmd1)
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
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
	} else if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	}

	// Second node should eventually become primary.
	t.Log("waiting for promotion of replica")
	waitForPrimary(t, cmd1)

	// Update record on the new primary.
	t.Log("updating record on new primary")
	if _, err := db1.Exec(`UPDATE t SET x = 200`); err != nil {
		t.Fatal(err)
	}

	// Reopen first node and ensure it propagates changes.
	t.Log("restarting first node as replica")
	cmd0 = runMountCommand(t, newMountCommand(t, dir0, cmd1))
	waitForSync(t, "db", cmd0, cmd1)

	db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	t.Log("verifying propagation of record update")
	if err := db0.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
}

func TestMultiNode_PrimaryFlipFlop(t *testing.T) {
	dirs := []string{t.TempDir(), t.TempDir()}
	cmds := []*main.MountCommand{runMountCommand(t, newMountCommand(t, dirs[0], nil)), nil}
	waitForPrimary(t, cmds[0])
	cmds[1] = runMountCommand(t, newMountCommand(t, dirs[1], cmds[0]))

	// Initialize schema.
	testingutil.WithTx(t, "sqlite3-persist-wal", filepath.Join(cmds[0].Config.FUSE.Dir, "db"), func(tx *sql.Tx) {
		if _, err := tx.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if _, err := tx.Exec(`CREATE INDEX t_x on t (x)`); err != nil {
			t.Fatal(err)
		}
	})
	waitForSync(t, "db", cmds...)

	for i := range cmds {
		t.Logf("CMD[%d]: %s (%v)", i, cmds[i].Store.ID(), cmds[i].Store.IsPrimary())
	}

	rand := rand.New(rand.NewSource(0))

	// Write new data to primary node.
	t.Logf("inserting data into node 0")
	testingutil.WithTx(t, "sqlite3-persist-wal", filepath.Join(cmds[0].Config.FUSE.Dir, "db"), func(tx *sql.Tx) {
		for j := 0; j < rand.Intn(20); j++ {
			buf := make([]byte, rand.Intn(256))
			_, _ = rand.Read(buf)
			if _, err := tx.Exec(`INSERT INTO t (x) VALUES (?)`, hex.EncodeToString(buf)); err != nil {
				t.Fatal(err)
			}
		}
	})
	waitForSync(t, "db", cmds...)

	// Demote primary & wait for replica promotion.
	cmds[0].Store.Demote()
	waitForPrimary(t, cmds[1])

	// Write new data to primary node.
	t.Logf("inserting data into node 1")
	testingutil.WithTx(t, "sqlite3-persist-wal", filepath.Join(cmds[1].Config.FUSE.Dir, "db"), func(tx *sql.Tx) {
		for j := 0; j < rand.Intn(20); j++ {
			buf := make([]byte, rand.Intn(256))
			_, _ = rand.Read(buf)
			if _, err := tx.Exec(`INSERT INTO t (x) VALUES (?)`, hex.EncodeToString(buf)); err != nil {
				t.Fatal(err)
			}
		}
	})
	waitForSync(t, "db", cmds...)

	// Demote new primary & wait for old primary promotion.
	cmds[1].Store.Demote()
	waitForPrimary(t, cmds[0])

	// Write new data to primary node.
	t.Logf("inserting data into node 0 again")
	testingutil.WithTx(t, "sqlite3-persist-wal", filepath.Join(cmds[0].Config.FUSE.Dir, "db"), func(tx *sql.Tx) {
		for j := 0; j < rand.Intn(20); j++ {
			buf := make([]byte, rand.Intn(256))
			_, _ = rand.Read(buf)
			if _, err := tx.Exec(`INSERT INTO t (x) VALUES (?)`, hex.EncodeToString(buf)); err != nil {
				t.Fatal(err)
			}
		}
	})
	waitForSync(t, "db", cmds...)

	// Verify both nodes are valid at the end.
	for _, cmd := range cmds {
		testingutil.WithTx(t, "sqlite3", filepath.Join(cmd.Config.FUSE.Dir, "db"), func(tx *sql.Tx) {
			var result string
			if err := tx.QueryRow(`PRAGMA integrity_check`).Scan(&result); err != nil {
				t.Fatal(err)
			} else if got, want := result, "ok"; got != want {
				t.Fatalf("result=%q, want %q", got, want)
			}
		})
	}
}

// Ensure two nodes that diverge will recover when the replica reconnects.
// This can occur if a primary commits a transaction before replicating, then
// loses its primary status, and a replica gets promoted and begins committing.
func TestMultiNode_PositionMismatchRecovery(t *testing.T) {
	t.Run("SameTXIDWithChecksumMismatch", func(t *testing.T) {
		dir0, dir1 := t.TempDir(), t.TempDir()
		cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
		waitForPrimary(t, cmd0)
		cmd1 := runMountCommand(t, newMountCommand(t, dir1, cmd0))
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

		// Create database on initial primary & sync.
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		waitForSync(t, "db", cmd0, cmd1)

		// Shutdown replica & issue another commit.
		t.Log("shutting down replica node")
		if err := cmd1.Close(); err != nil {
			t.Fatal(err)
		}
		t.Log("create an unreplicated transaction on primary node")
		if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}

		// Shutdown primary with unreplicated transaction.
		t.Log("shutting down primary node")
		if err := db0.Close(); err != nil {
			t.Fatal(err)
		} else if err := cmd0.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart replica and wait for it to become primary.
		t.Log("restarting second node")
		cmd1 = runMountCommand(t, newMountCommand(t, dir1, cmd1))
		waitForPrimary(t, cmd1)

		// Issue a different transaction on new primary.
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		if _, err := db1.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		}

		// Verify result on new primary.
		var x int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 400; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}

		// Reopen first node and ensure it re-snapshots.
		t.Log("restarting first node")
		cmd0 = runMountCommand(t, newMountCommand(t, dir0, cmd1))
		waitForSync(t, "db", cmd0, cmd1)

		t.Log("verify first node snapshots from second node")
		db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 400; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}
	})

	t.Run("ReplicaWithHigherTXID", func(t *testing.T) {
		dir0, dir1 := t.TempDir(), t.TempDir()
		cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
		waitForPrimary(t, cmd0)
		cmd1 := runMountCommand(t, newMountCommand(t, dir1, cmd0))
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

		// Create database on initial primary & sync.
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		waitForSync(t, "db", cmd0, cmd1)

		// Shutdown replica & issue another commit.
		t.Log("shutting down replica node")
		if err := cmd1.Close(); err != nil {
			t.Fatal(err)
		}
		t.Log("create multiple unreplicated transactions on primary node")
		if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		}

		// Shutdown primary with unreplicated transaction.
		t.Log("shutting down primary node")
		if err := db0.Close(); err != nil {
			t.Fatal(err)
		} else if err := cmd0.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart replica and wait for it to become primary.
		t.Log("restarting second node")
		cmd1 = runMountCommand(t, newMountCommand(t, dir1, cmd1))
		waitForPrimary(t, cmd1)

		// Issue a different transaction on new primary.
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		if _, err := db1.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		}

		// Verify result on new primary.
		var x int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 400; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}

		// Reopen first node and ensure it re-snapshots.
		t.Log("restarting first node")
		cmd0 = runMountCommand(t, newMountCommand(t, dir0, cmd1))
		waitForSync(t, "db", cmd0, cmd1)

		t.Log("verify first node snapshots from second node")
		db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 400; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}
	})

	t.Run("PreApplyChecksumMismatch", func(t *testing.T) {
		dir0, dir1 := t.TempDir(), t.TempDir()
		cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
		waitForPrimary(t, cmd0)
		cmd1 := runMountCommand(t, newMountCommand(t, dir1, cmd0))
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

		// Create database on initial primary & sync.
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		waitForSync(t, "db", cmd0, cmd1)

		// Shutdown replica & issue another commit.
		t.Log("shutting down replica node")
		if err := cmd1.Close(); err != nil {
			t.Fatal(err)
		}
		t.Log("create multiple unreplicated transactions on primary node")
		if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}

		// Shutdown primary with unreplicated transaction.
		t.Log("shutting down primary node")
		if err := db0.Close(); err != nil {
			t.Fatal(err)
		} else if err := cmd0.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart replica and wait for it to become primary.
		t.Log("restarting second node")
		cmd1 = runMountCommand(t, newMountCommand(t, dir1, cmd1))
		waitForPrimary(t, cmd1)

		// Issue a different transaction on new primary.
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		if _, err := db1.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		} else if _, err := db1.Exec(`INSERT INTO t VALUES (400)`); err != nil {
			t.Fatal(err)
		}

		// Verify result on new primary.
		var x int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 800; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}

		// Reopen first node and ensure it re-snapshots.
		t.Log("restarting first node")
		cmd0 = runMountCommand(t, newMountCommand(t, dir0, cmd1))
		waitForSync(t, "db", cmd0, cmd1)

		t.Log("verify first node snapshots from second node")
		db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 800; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}
	})
}

func TestMultiNode_EnsureReadOnlyReplica(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Ensure we cannot write to the replica.
	waitForSync(t, "db", cmd0, cmd1)
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	_, err := db1.Exec(`INSERT INTO t VALUES (200)`)
	if testingutil.IsWALMode() {
		if err == nil || err.Error() != `disk I/O error` {
			t.Fatalf("unexpected error: %s", err)
		}
	} else {
		if err == nil || err.Error() != `attempt to write a readonly database` {
			t.Fatalf("unexpected error: %s", err)
		}
	}

}

func TestMultiNode_Candidate(t *testing.T) {
	dir0, dir1 := t.TempDir(), t.TempDir()
	cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
	waitForPrimary(t, cmd0)
	cmd1 := newMountCommand(t, dir1, cmd0)
	cmd1.Config.Lease.Candidate = false
	runMountCommand(t, cmd1)
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a database and wait for sync.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	// Stop the primary.
	t.Log("shutting down primary node")
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	}

	// Second node is NOT a candidate so it should not become primary.
	t.Log("waiting to ensure replica is not promoted...")
	time.Sleep(3 * time.Second)
	if cmd1.Store.IsPrimary() {
		t.Fatalf("replica should not have been promoted to primary")
	}

	// Reopen first node and ensure it can become primary again.
	t.Log("restarting first node as replica")
	cmd0 = runMountCommand(t, newMountCommand(t, dir0, cmd1))
	waitForPrimary(t, cmd0)
}

func TestMultiNode_StaticLeaser(t *testing.T) {
	dir0, dir1 := t.TempDir(), t.TempDir()
	cmd0 := newMountCommand(t, dir0, nil)
	cmd0.Config.HTTP.Addr = ":20808"
	cmd0.Config.Lease.Type = "static"
	cmd0.Config.Lease.Hostname = "cmd0"
	cmd0.Config.Lease.AdvertiseURL = "http://localhost:20808"
	cmd0.Config.Lease.Candidate = true // primary

	runMountCommand(t, cmd0)
	waitForPrimary(t, cmd0)

	cmd1 := newMountCommand(t, dir1, cmd0)
	cmd1.Config.Lease.Type = "static"
	cmd1.Config.Lease.Hostname = "cmd0"
	cmd1.Config.Lease.AdvertiseURL = "http://localhost:20808"
	cmd1.Config.Lease.Candidate = false // replica
	runMountCommand(t, cmd1)

	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a database and wait for sync.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	// Verify primary has no ".primary" file.
	if _, err := os.ReadFile(filepath.Join(cmd0.FileSystem.Path(), ".primary")); !os.IsNotExist(err) {
		t.Fatal("expected no primary file on the primary node")
	}

	// Verify replica sees the primary hostname.
	if b, err := os.ReadFile(filepath.Join(cmd1.FileSystem.Path(), ".primary")); err != nil {
		t.Fatal(err)
	} else if got, want := string(b), "cmd0\n"; got != want {
		t.Fatalf("primary=%q, want %q", got, want)
	}

	// Stop the primary.
	t.Log("shutting down primary node")
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	}

	// Second node is NOT a candidate so it should not become primary.
	t.Log("waiting to ensure replica is not promoted...")
	time.Sleep(1 * time.Second)
	if cmd1.Store.IsPrimary() {
		t.Fatalf("replica should not have been promoted to primary")
	}

	// Reopen first node and ensure it can become primary again.
	t.Log("restarting first node as replica")
	cmd0 = newMountCommand(t, dir0, cmd1)
	cmd0.Config.HTTP.Addr = ":20808"
	cmd0.Config.Lease.Type = "static"
	cmd0.Config.Lease.Hostname = "cmd0"
	cmd0.Config.Lease.AdvertiseURL = "http://localhost:20808"
	cmd0.Config.Lease.Candidate = true
	runMountCommand(t, cmd0)
	waitForPrimary(t, cmd0)
}

func TestMultiNode_EnforceRetention(t *testing.T) {
	cmd := newMountCommand(t, t.TempDir(), nil)
	cmd.Config.Data.Retention = 1 * time.Second
	cmd.Config.Data.RetentionMonitorInterval = 100 * time.Millisecond
	waitForPrimary(t, runMountCommand(t, cmd))
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd.Config.FUSE.Dir, "db"))

	// Create multiple transactions.
	txID := cmd.Store.DB("db").TXID()
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Wait for retention to occur.
	t.Logf("waiting for retention enforcement")
	time.Sleep(3 * time.Second)

	// Ensure only one LTX file remains.
	if ents, err := cmd.Store.DB("db").ReadLTXDir(); err != nil {
		t.Fatal(err)
	} else if got, want := len(ents), 1; got != want {
		t.Fatalf("n=%d, want %d", got, want)
	} else if got, want := ents[0].Name(), fmt.Sprintf(`000000000000000%d-000000000000000%d.ltx`, txID+3, txID+3); got != want {
		t.Fatalf("ent[0]=%s, want %s", got, want)
	}
}

func TestMultiNode_Proxy(t *testing.T) {
	t.Run("PrimaryRedirection", func(t *testing.T) {
		// Start primary application & mount.
		s0 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			body, _ := io.ReadAll(r.Body)
			w.Header().Set("myheader", "123")
			w.WriteHeader(201)
			_, _ = w.Write([]byte("primary ok: " + string(body)))
		}))
		defer s0.Close()

		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Lease.Hostname = "MYPRIMARY"
		cmd0.Config.Proxy.Target = strings.TrimPrefix(s0.URL, "http://")
		cmd0.Config.Proxy.DB = "db"
		cmd0.Config.Proxy.Addr = ":0"
		runMountCommand(t, cmd0)
		waitForPrimary(t, cmd0)

		// Create a simple table with a single value.
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}

		// Start replica application & mount.
		replicaCh := make(chan struct{})
		s1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			close(replicaCh)
		}))
		defer s1.Close()

		cmd1 := newMountCommand(t, t.TempDir(), cmd0)
		cmd1.Config.Proxy.Target = strings.TrimPrefix(s1.URL, "http://")
		cmd1.Config.Proxy.DB = "db"
		cmd1.Config.Proxy.Addr = ":0"
		runMountCommand(t, cmd1)
		waitForSync(t, "db", cmd0, cmd1)

		// Make a write request the replica proxy; verify fly-proxy is set.
		resp, err := http.Post(cmd1.ProxyServer.URL(), "text/html", nil)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		if got, want := resp.Header.Get("fly-replay"), "instance=MYPRIMARY"; got != want {
			t.Fatalf("fly-replay=%q, want %q", got, want)
		}

		// Ensure replica server is not contacted.
		select {
		case <-replicaCh:
			t.Fatal("should not send request to replica")
		default:
		}

		// Make a write request the primary proxy; verify application receives request.
		resp, err = http.Post(cmd0.ProxyServer.URL(), "text/html", strings.NewReader("foobar"))
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		if got, want := resp.Header.Get("fly-replay"), ""; got != want {
			t.Fatalf("fly-replay=%q, want %q", got, want)
		}
		if got, want := resp.StatusCode, 201; got != want {
			t.Fatalf("status=%d, want %d", got, want)
		}
		if got, want := resp.Header.Get("myheader"), "123"; got != want {
			t.Fatalf("myheader=%q, want %q", got, want)
		}
		if b, err := io.ReadAll(resp.Body); err != nil {
			t.Fatal(err)
		} else if got, want := string(b), "primary ok: foobar"; got != want {
			t.Fatalf("body=%q, want %q", got, want)
		}
	})
}

// Ensure multiple nodes can run in a cluster for an extended period of time.
func TestFunctional_OK(t *testing.T) {
	if *funTime <= 0 {
		t.Skip("-funtime unset, skipping functional test")
	}
	done := make(chan struct{})
	go func() { <-time.After(*funTime); close(done) }()

	// Configure nodes with a low retention.
	newFunCmd := func(peer *main.MountCommand) *main.MountCommand {
		cmd := newMountCommand(t, t.TempDir(), peer)
		cmd.Config.Data.Retention = 2 * time.Second
		cmd.Config.Data.RetentionMonitorInterval = 1 * time.Second
		return cmd
	}

	// Initialize nodes.
	var cmds []*main.MountCommand
	cmds = append(cmds, runMountCommand(t, newFunCmd(nil)))
	waitForPrimary(t, cmds[0])
	cmds = append(cmds, runMountCommand(t, newFunCmd(cmds[0])))

	// Create schema.
	db := testingutil.OpenSQLDB(t, filepath.Join(cmds[0].Config.FUSE.Dir, "db"))
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, value TEXT)`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmds...)

	// Continually run queries against nodes.
	g, ctx := errgroup.WithContext(context.Background())
	for i, m := range cmds {
		i, m := i, m
		g.Go(func() error {
			db := testingutil.OpenSQLDB(t, filepath.Join(m.Config.FUSE.Dir, "db"))
			ticker := time.NewTicker(10 * time.Millisecond)
			defer ticker.Stop()

			for j := 0; ; j++ {
				select {
				case <-done:
					return nil // test time has elapsed
				case <-ctx.Done():
					return nil // another goroutine failed
				case <-ticker.C:
					if m.Store.IsPrimary() {
						if _, err := db.Exec(`INSERT INTO t (value) VALUES (?)`, strings.Repeat("x", 200)); err != nil {
							return fmt.Errorf("cannot insert (node %d, iter %d): %s", i, j, err)
						}
					}

					var id int
					var value string
					if err := db.QueryRow(`SELECT id, value FROM t ORDER BY id DESC LIMIT 1`).Scan(&id, &value); err != nil && err != sql.ErrNoRows {
						return fmt.Errorf("cannot query (node %d, iter %d): %s", i, j, err)
					}
				}
			}
		})
	}

	// Ensure we have the same data once we resync.
	if err := g.Wait(); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmds...)

	counts := make([]int, len(cmds))
	for i, m := range cmds {
		db := testingutil.OpenSQLDB(t, filepath.Join(m.Config.FUSE.Dir, "db"))
		if err := db.QueryRow(`SELECT COUNT(id) FROM t`).Scan(&counts[i]); err != nil {
			t.Fatal(err)
		}

		if i > 0 && counts[i-1] != counts[i] {
			t.Errorf("count mismatch(%d,%d): %d <> %d", i-1, i, counts[i-1], counts[i])
		}
	}
}

func TestMountCommand_Validate(t *testing.T) {
	t.Run("ErrFUSEDirectoryRequired", func(t *testing.T) {
		cmd := main.NewMountCommand()
		if err := cmd.Validate(context.Background()); err == nil || err.Error() != `fuse directory required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrDataDirectoryRequired", func(t *testing.T) {
		cmd := main.NewMountCommand()
		cmd.Config.FUSE.Dir = t.TempDir()
		if err := cmd.Validate(context.Background()); err == nil || err.Error() != `data directory required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrMatchingDirs", func(t *testing.T) {
		cmd := main.NewMountCommand()
		cmd.Config.FUSE.Dir = t.TempDir()
		cmd.Config.Data.Dir = cmd.Config.FUSE.Dir
		if err := cmd.Validate(context.Background()); err == nil || err.Error() != `fuse directory and data directory cannot be the same path` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

//go:embed etc/litefs.yml
var litefsConfig []byte

func TestUnmarshalConfig(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		config := main.NewConfig()
		if err := main.UnmarshalConfig(&config, litefsConfig, false); err != nil {
			t.Fatal(err)
		}
		if got, want := config.Data.Dir, "/var/lib/litefs"; got != want {
			t.Fatalf("FUSE.Dir=%s, want %s", got, want)
		}
		if got, want := config.FUSE.Dir, "/litefs"; got != want {
			t.Fatalf("FUSE.Dir=%s, want %s", got, want)
		}
		if got, want := config.FUSE.Debug, false; got != want {
			t.Fatalf("Debug=%v, want %v", got, want)
		}
		if got, want := config.HTTP.Addr, ":20202"; got != want {
			t.Fatalf("HTTP.Addr=%s, want %s", got, want)
		}
		if got, want := config.Lease.Type, "consul"; got != want {
			t.Fatalf("Lease.Type=%s, want %s", got, want)
		}
		if got, want := config.Lease.Hostname, "myhost"; got != want {
			t.Fatalf("Lease.Hostname=%s, want %s", got, want)
		}
		if got, want := config.Lease.AdvertiseURL, "http://myhost:20202"; got != want {
			t.Fatalf("Lease.AdvertiseURL=%s, want %s", got, want)
		}
		if got, want := config.Lease.Consul.URL, "http://myhost:8500"; got != want {
			t.Fatalf("Lease.Consul.URL=%s, want %s", got, want)
		}
		if got, want := config.Lease.Consul.Key, "litefs/primary"; got != want {
			t.Fatalf("Lease.Consul.Key=%s, want %s", got, want)
		}
		if got, want := config.Lease.Consul.TTL, 10*time.Second; got != want {
			t.Fatalf("Lease.Consul.TTL=%s, want %s", got, want)
		}
		if got, want := config.Lease.Consul.LockDelay, 1*time.Second; got != want {
			t.Fatalf("Lease.Consul.LockDelay=%s, want %s", got, want)
		}
		if got, want := config.Lease.Candidate, true; got != want {
			t.Fatalf("Lease.Candidate=%v, want %v", got, want)
		}
	})

	t.Run("ErrUnknownField", func(t *testing.T) {
		config := main.NewConfig()
		if err := main.UnmarshalConfig(&config, []byte("data:\n  bar: 123"), false); err == nil || err.Error() != "yaml: unmarshal errors:\n  line 2: field bar not found in type main.DataConfig" {
			t.Fatalf("unexpected error: %q", err)
		}
	})
}

func TestExpandEnv(t *testing.T) {
	_ = os.Setenv("LITEFS_FOO", "foo")
	_ = os.Setenv("LITEFS_FOO2", "foo")
	_ = os.Setenv("LITEFS_BAR", "bar baz")

	t.Run("UnbracedVar", func(t *testing.T) {
		if got, want := main.ExpandEnv("$LITEFS_FOO"), `foo`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("BracedVar", func(t *testing.T) {
		if got, want := main.ExpandEnv("${LITEFS_FOO}"), `foo`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if got, want := main.ExpandEnv("${ LITEFS_FOO }"), `foo`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("SingleQuoteExpr", func(t *testing.T) {
		if got, want := main.ExpandEnv("${ LITEFS_FOO == 'foo' }"), `true`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if got, want := main.ExpandEnv("${ LITEFS_FOO != 'foo' }"), `false`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("DoubleQuoteExpr", func(t *testing.T) {
		if got, want := main.ExpandEnv(`${ LITEFS_BAR == "bar baz" }`), `true`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if got, want := main.ExpandEnv(`${ LITEFS_BAR != "" }`), `true`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	t.Run("VarExpr", func(t *testing.T) {
		if got, want := main.ExpandEnv("${ LITEFS_FOO == LITEFS_FOO2 }"), `true`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
		if got, want := main.ExpandEnv("${ LITEFS_FOO != LITEFS_BAR }"), `true`; got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

func newMountCommand(tb testing.TB, dir string, peer *main.MountCommand) *main.MountCommand {
	tb.Helper()

	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatalf("cannot remove temp directory: %s", err)
		}
	})

	cmd := main.NewMountCommand()
	cmd.Config.FUSE.Dir = filepath.Join(dir, "mnt")
	cmd.Config.FUSE.Debug = *fuseDebug
	cmd.Config.Data.Dir = filepath.Join(dir, "data")
	cmd.Config.Data.Compress = testingutil.Compress()
	cmd.Config.StrictVerify = true
	cmd.Config.HTTP.Addr = ":0"
	cmd.Config.Lease.ReconnectDelay = 10 * time.Millisecond
	cmd.Config.Lease.DemoteDelay = 1 * time.Second
	cmd.Config.Lease.Type = "consul"
	cmd.Config.Lease.Consul.URL = "http://localhost:8500"
	cmd.Config.Lease.Consul.Key = fmt.Sprintf("%x", rand.Int31())
	cmd.Config.Lease.Consul.TTL = 10 * time.Second
	cmd.Config.Lease.Consul.LockDelay = 1 * time.Second

	// Use peer's consul key, if passed in.
	if peer != nil {
		cmd.Config.Lease.Consul.Key = peer.Config.Lease.Consul.Key
	}

	// Generate URL from HTTP server after port is assigned.
	cmd.AdvertiseURLFn = func() string {
		return fmt.Sprintf("http://localhost:%d", cmd.HTTPServer.Port())
	}

	return cmd
}

func runMountCommand(tb testing.TB, cmd *main.MountCommand) *main.MountCommand {
	tb.Helper()

	if err := cmd.Run(context.Background()); err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() {
		if err := cmd.Close(); err != nil {
			log.Printf("cannot close mount command: %s", err)
		}
	})

	return cmd
}

// waitForPrimary waits for m to obtain the primary lease.
func waitForPrimary(tb testing.TB, cmd *main.MountCommand) {
	tb.Helper()
	tb.Logf("waiting for primary...")

	testingutil.RetryUntil(tb, 1*time.Millisecond, 5*time.Second, func() error {
		tb.Helper()

		if !cmd.Store.IsPrimary() {
			return fmt.Errorf("not primary")
		}
		return nil
	})
}

// waitForSync waits for all processes to sync to the same TXID.
func waitForSync(tb testing.TB, name string, cmds ...*main.MountCommand) {
	tb.Helper()

	testingutil.RetryUntil(tb, 1*time.Millisecond, 5*time.Second, func() error {
		tb.Helper()

		db0 := cmds[0].Store.DB(name)
		if db0 == nil {
			return fmt.Errorf("no database on mount[0]")
		}

		txID := db0.TXID()
		for i, cmd := range cmds {
			db := cmd.Store.DB(name)
			if db == nil {
				return fmt.Errorf("no database on mount[%d]", i)
			}

			if got, want := db.TXID(), txID; got != want {
				return fmt.Errorf("waiting for sync on db(%d): got=%d, want=%d", i, got, want)
			}
		}

		tb.Logf("%d processes synced for db %q at tx %d", len(cmds), name, txID)
		return nil
	})
}
