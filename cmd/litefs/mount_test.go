// go:build linux
package main_test

import (
	"bytes"
	"context"
	"crypto/tls"
	"database/sql"
	_ "embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/superfly/litefs"
	main "github.com/superfly/litefs/cmd/litefs"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/litefs/internal/testingutil"
	"github.com/superfly/litefs/mock"
	"github.com/superfly/ltx"
	"golang.org/x/exp/slog"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
	"golang.org/x/sys/unix"
)

func init() {
	if err := os.Unsetenv("LITEFS_CLOUD_TOKEN"); err != nil {
		panic("cannot unset LITEFS_CLOUD_TOKEN environment variable")
	}
}

func TestSingleNode_OK(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	t.Log("creating table...")
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	t.Log("inserting row...")
	if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Ensure we can retrieve the data back from the database.
	t.Log("querying database...")
	var x int
	if err := db.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
	t.Log("test complete")
}

func TestSingleNode_WithCLI(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	dsn := filepath.Join(cmd0.Config.FUSE.Dir, "db")
	db := testingutil.OpenSQLDB(t, dsn)

	// Create a simple table with a single value.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}

	// Run for a fixed period of time.
	done := make(chan struct{})
	time.AfterFunc(5*time.Second, func() { close(done) })

	// Continuously insert values from Go driver.
	var g errgroup.Group
	g.Go(func() error {
		for i := 0; ; i++ {
			select {
			case <-done:
				return nil
			default:
				if _, err := db.Exec(`INSERT INTO t VALUES (?)`, i); err != nil {
					t.Errorf("INSERT#%d: err=%s", i, err)
				}
			}
		}
	})

	// Continuously query for max value from CLI.
	g.Go(func() error {
		for i := 0; ; i++ {
			select {
			case <-done:
				return nil
			default:
				cliCmd := exec.Command("sqlite3", dsn, "PRAGMA busy_timeout=5000; SELECT MAX(x) FROM t")
				buf, err := cliCmd.CombinedOutput()
				if err != nil {
					t.Errorf("CLI#%d: err=%s", i, err)
				}
				t.Logf("sqlite3: max=%q", strings.TrimSpace(string(buf)))
			}
		}
	})

	if err := g.Wait(); err != nil {
		t.Fatal(err)
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
	txID := ltx.TXID(2)
	if testingutil.IsWALMode() {
		txID++
	}

	// Corrupt one of the LTX files.
	if f, err := os.OpenFile(cmd0.Store.DB("db").LTXPath(txID, txID), os.O_RDWR, 0o666); err != nil {
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
	if f, err := os.OpenFile(cmd0.Store.DB("db").DatabasePath(), os.O_RDWR, 0o666); err != nil {
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
	if f, err := os.OpenFile(cmd0.Store.DB("db").DatabasePath(), os.O_RDWR, 0o666); err != nil {
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
		if err := cmd0.Run(context.Background()); err == nil || err.Error() != `cannot open store: open databases: open database("db"): recover ltx: database checksum 9d81a60d39fb4760 on TXID 0000000000000003 does not match LTX post-apply checksum ce5a5d55e91b3cd1` {
			t.Fatalf("unexpected error: %s", err)
		}
	case "wal":
		if err := cmd0.Run(context.Background()); err == nil || err.Error() != `cannot open store: open databases: open database("db"): recover ltx: database checksum a9e884061ea4e488 on TXID 0000000000000004 does not match LTX post-apply checksum fa337f5ece449f39` {
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

func TestSingleNode_DropDB(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	dsn := filepath.Join(cmd0.Config.FUSE.Dir, "db")
	db := testingutil.OpenSQLDB(t, dsn)

	// Create a simple table with a single value.
	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}

	// Verify file is exists.
	if _, err := os.Stat(dsn); err != nil {
		t.Fatal(err)
	}

	// Remove database.
	if err := os.Remove(dsn); err != nil {
		t.Fatal(err)
	}

	// Verify database and related files are gone.
	for _, suffix := range []string{"", "-journal", "-wal", "-shm"} {
		filename := dsn + suffix
		if _, err := os.Stat(filename); !os.IsNotExist(err) {
			t.Fatalf("expected no %q, got %v", filepath.Base(filename), err)
		}
	}
}

func TestSingleNode_ErrCommitWAL(t *testing.T) {
	if !testingutil.IsWALMode() {
		t.Skip("test failure only applies to WAL mode, skipping")
	}

	mos := mock.NewOS()
	dir := t.TempDir()
	cmd0 := newMountCommand(t, dir, nil)
	cmd0.OS = mos
	runMountCommand(t, cmd0)
	db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	var exitCode int
	cmd0.Store.Exit = func(code int) {
		exitCode = code
	}
	mos.OpenFunc = func(op, name string) (*os.File, error) {
		if op == "COMMITWAL:WAL" {
			return nil, fmt.Errorf("marker")
		}
		return os.Open(name)
	}
	if _, err := db.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	if got, want := exitCode, 99; got != want {
		t.Fatalf("exit=%d, want=%d", got, want)
	}
	if err := db.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd0.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart the mount command.
	cmd0 = runMountCommand(t, newMountCommand(t, dir, nil))
	db = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Ensure the second insert was not processed.
	var x int
	if err := db.QueryRow(`SELECT SUM(x) FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
}

func TestSingleNode_BackupClient(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Backup = main.BackupConfig{
			Type:  "file",
			Path:  t.TempDir(),
			Delay: 10 * time.Millisecond,
		}
		runMountCommand(t, cmd0)

		// Create a simple table with a single value.
		db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)

		// Insert more data.
		if _, err := db.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)
	})

	// Ensure LiteFS can start from a snapshot when it doesn't have LTX TXID#1 available.
	t.Run("InitiateSnapshot", func(t *testing.T) {
		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Data.Retention = 1 * time.Microsecond
		cmd0.Config.Backup = main.BackupConfig{Type: "file", Path: t.TempDir(), Delay: 1 * time.Millisecond}
		runMountCommand(t, cmd0)

		// Create a simple table with a single value.
		db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
				t.Fatal(err)
			}
		}

		// Enforce retention to remove initial LTX file.
		time.Sleep(cmd0.Config.Data.Retention)
		if err := cmd0.Store.EnforceRetention(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Sync to backup.
		if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Insert more data.
		if _, err := db.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		} else if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)
	})

	// Ensure LiteFS can send a deleted database to backup.
	t.Run("InitiateSnapshot/Empty", func(t *testing.T) {
		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Data.Retention = 1 * time.Microsecond
		cmd0.Config.Backup = main.BackupConfig{Type: "file", Path: t.TempDir(), Delay: 1 * time.Millisecond}
		runMountCommand(t, cmd0)

		// Create a simple table and delete the database.
		db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if err := db.Close(); err != nil {
			t.Fatal(err)
		} else if err := os.Remove(filepath.Join(cmd0.Config.FUSE.Dir, "db")); err != nil {
			t.Fatal(err)
		}

		// Enforce retention to remove initial LTX file.
		time.Sleep(cmd0.Config.Data.Retention)
		if err := cmd0.Store.EnforceRetention(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Sync to backup.
		if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Recreate database
		db = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db.Exec(`CREATE TABLE v (y)`); err != nil {
			t.Fatal(err)
		}
		if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)
	})

	// Ensure LiteFS can restore a database from backup if it doesn't exist locally.
	t.Run("RestoreFromBackup/NoLocalDatabase", func(t *testing.T) {
		backupDir := t.TempDir()
		backupConfig := main.BackupConfig{Type: "file", Path: backupDir, Delay: 1 * time.Millisecond}

		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Backup = backupConfig
		runMountCommand(t, cmd0)

		// Create a simple table with a single value.
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Close(); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)

		// Shutdown mount and restart a new one with no data.
		if err := cmd0.Close(); err != nil {
			t.Fatal(err)
		}
		t.Logf("shutdown original mount, starting new one")

		cmd1 := newMountCommand(t, t.TempDir(), nil)
		cmd1.Config.Backup = backupConfig
		runMountCommand(t, cmd1)
		waitForBackupSync(t, cmd1)

		// Query database to ensure it exists.
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		var x int
		if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
			t.Fatal(err)
		} else if got, want := x, 100; got != want {
			t.Fatalf("x=%d, want %d", got, want)
		}
	})

	// Ensure LiteFS can restore a database from backup if remote position is ahead of local.
	t.Run("RestoreFromBackup/RemoteAhead", func(t *testing.T) {
		dir0, dir1 := t.TempDir(), t.TempDir()
		backupDir := t.TempDir()
		backupConfig := main.BackupConfig{Type: "file", Path: backupDir, Delay: 1 * time.Millisecond}

		cmd0 := newMountCommand(t, dir0, nil)
		cmd0.Config.Backup = backupConfig
		waitForPrimary(t, runMountCommand(t, cmd0))

		cmd1 := newMountCommand(t, dir1, cmd0)
		cmd1.Config.Backup = backupConfig
		runMountCommand(t, cmd1)

		// Create a simple table with a single value.
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		waitForSync(t, "db", cmd0, cmd1)
		waitForBackupSync(t, cmd0)

		// Shutdown the replica.
		t.Logf("shutdown replica")
		if err := cmd1.Close(); err != nil {
			t.Fatal(err)
		}

		// Continue writing to the primary.
		t.Logf("write additional rows to primary & sync")
		if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Close(); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)

		// Shutdown the primary.
		t.Logf("shutdown primary")
		if err := cmd0.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart the previous replica so it becomes the new primary.
		t.Logf("restart replica and wait for promotion")
		cmd1 = newMountCommand(t, dir1, nil)
		cmd1.Config.Backup = backupConfig
		waitForPrimary(t, runMountCommand(t, cmd1))
		waitForBackupSync(t, cmd1)

		// Ensure that the database is restored from the last sync of the original primary.
		t.Logf("ensure database is restored")
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		var sum int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 600; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
	})

	// Ensure LiteFS can restore a database from backup if remote position is ahead of local.
	t.Run("RestoreFromBackup/ChecksumMismatch", func(t *testing.T) {
		dir0, dir1 := t.TempDir(), t.TempDir()
		backupDir := t.TempDir()
		backupConfig := main.BackupConfig{Type: "file", Path: backupDir, Delay: 1 * time.Millisecond}

		cmd0 := newMountCommand(t, dir0, nil)
		cmd0.Config.Backup = backupConfig
		waitForPrimary(t, runMountCommand(t, cmd0))

		cmd1 := newMountCommand(t, dir1, cmd0)
		cmd1.Config.Backup = backupConfig
		runMountCommand(t, cmd1)

		// Create a simple table with a single value.
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		waitForSync(t, "db", cmd0, cmd1)
		waitForBackupSync(t, cmd0)

		// Shutdown the replica.
		t.Logf("shutdown replica")
		if err := cmd1.Close(); err != nil {
			t.Fatal(err)
		}

		// Write two transactions to the primary.
		t.Logf("write additional rows to primary & sync")
		if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Close(); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd0)

		// Shutdown the primary.
		t.Logf("shutdown primary")
		if err := cmd0.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart the previous replica so it becomes the new primary.
		t.Logf("restart replica and wait for promotion")
		cmd1 = newMountCommand(t, dir1, nil)
		waitForPrimary(t, runMountCommand(t, cmd1))
		cmd1.Store.BackupClient = litefs.NewFileBackupClient(backupDir)

		// Insert two transactions before we perform a backup sync.
		t.Logf("writing two transactions from new primary")
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		if _, err := db1.Exec(`INSERT INTO t VALUES (400)`); err != nil {
			t.Fatal(err)
		} else if _, err := db1.Exec(`INSERT INTO t VALUES (500)`); err != nil {
			t.Fatal(err)
		} else if err := db1.Close(); err != nil {
			t.Fatal(err)
		}

		// Sync up with backup now that we have the same TXID but different checksum.
		t.Logf("sync new primary with backup")
		if err := cmd1.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Ensure that the database is restored from the last sync of the original primary.
		t.Logf("ensure database is reverted")
		db1 = testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		var sum int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 600; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
	})

	// Ensure LiteFS can restore a database from backup if there are missing LTX files.
	t.Run("RestoreFromBackup/LTXNotFound", func(t *testing.T) {
		dir0, backupDir := t.TempDir(), t.TempDir()

		cmd0 := newMountCommand(t, dir0, nil)
		waitForPrimary(t, runMountCommand(t, cmd0))
		cmd0.Store.BackupClient = litefs.NewFileBackupClient(backupDir)

		// Create a simple table with a single value.
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}

		// Sync to backup.
		if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Write two transactions to the primary but don't sync.
		t.Logf("write additional rows to primary & sync")
		if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}
		pos := cmd0.Store.DB("db").Pos()

		if _, err := db0.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Close(); err != nil {
			t.Fatal(err)
		}

		// Remove one of the LTX files so we have a gap in the chain.
		if err := os.Remove(cmd0.Store.DB("db").LTXPath(pos.TXID, pos.TXID)); err != nil {
			t.Fatal(err)
		}

		// Sync to backup.
		if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Ensure that the database is restored from the last sync of the original primary.
		t.Logf("ensure database is reverted")
		db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		var sum int
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 100; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
	})

	t.Run("DefaultLiteFSCloudURL", func(t *testing.T) {
		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Backup = main.BackupConfig{Type: "litefs-cloud"}
		if err := cmd0.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := internal.Close(cmd0); err != nil {
			t.Fatal(err)
		}

		if got, want := cmd0.Config.Backup, (main.BackupConfig{
			Type: "litefs-cloud",
			URL:  "https://litefs.fly.io",
		}); got != want {
			t.Fatalf("config=%#v, want %#v", got, want)
		}
	})

	t.Run("InitFromEnv", func(t *testing.T) {
		if err := os.Setenv("LITEFS_CLOUD_TOKEN", "TOKENDATA"); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = os.Unsetenv("LITEFS_CLOUD_TOKEN") }()

		// Run and stop command so it applies defaults based on env var.
		cmd0 := newMountCommand(t, t.TempDir(), nil)
		if err := cmd0.Run(context.Background()); err != nil {
			t.Fatal(err)
		} else if err := internal.Close(cmd0); err != nil {
			t.Fatal(err)
		}

		if got, want := cmd0.Config.Backup, (main.BackupConfig{
			Type:             "litefs-cloud",
			URL:              "https://litefs.fly.io",
			AuthToken:        "TOKENDATA",
			Delay:            litefs.DefaultBackupDelay,
			FullSyncInterval: litefs.DefaultBackupFullSyncInterval,
		}); got != want {
			t.Fatalf("config=%#v, want %#v", got, want)
		}
	})

	// Ensure LiteFS periodically syncs with the backup server to detect restores.
	t.Run("FullSync", func(t *testing.T) {
		backupConfig := main.BackupConfig{
			Type:             "file",
			Path:             t.TempDir(),
			Delay:            10 * time.Microsecond,
			FullSyncInterval: 10 * time.Millisecond,
		}

		cmd0 := newMountCommand(t, t.TempDir(), nil)
		cmd0.Config.Backup = backupConfig
		runMountCommand(t, cmd0)
		waitForPrimary(t, cmd0)

		// Create a simple table with a single value.
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		} else if err := db0.Close(); err != nil {
			t.Fatal(err)
		}

		// Sync to backup.
		if err := cmd0.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}

		// Start replica, sync & shutdown.
		dir1 := t.TempDir()
		cmd1 := newMountCommand(t, dir1, cmd0)
		cmd1.Config.Backup = backupConfig
		runMountCommand(t, cmd1)
		waitForSync(t, "db", cmd0, cmd1)
		if err := cmd1.Close(); err != nil {
			t.Fatal(err)
		}

		// Restart replica as its own cluster so we can update the backup while
		// the old primary is still live.
		cmd1 = newMountCommand(t, dir1, nil)
		cmd1.Config.Backup = backupConfig
		runMountCommand(t, cmd1)
		waitForPrimary(t, cmd1)

		// Insert data into second primary.
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		if _, err := db1.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		} else if err := db1.Close(); err != nil {
			t.Fatal(err)
		}
		if err := cmd1.Store.SyncBackup(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitForBackupSync(t, cmd1)

		// Wait a moment for the first primary to pick up the changes.
		time.Sleep(1 * time.Second)

		db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		var n int
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&n); err != nil {
			t.Fatal(err)
		} else if got, want := n, 300; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
	})
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

func TestMultiNode_Drop(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))

	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := db0.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	// Ensure replica can read data.
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	var x int
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 100; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	// Remove database.
	if err := os.Remove(filepath.Join(cmd0.Config.FUSE.Dir, "db")); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	// Ensure primary & replica database has been deleted.
	if _, err := os.Stat(filepath.Join(cmd0.Config.FUSE.Dir, "db")); !os.IsNotExist(err) {
		t.Fatalf("expected no primary db, got %#v", err)
	}
	if _, err := os.Stat(filepath.Join(cmd1.Config.FUSE.Dir, "db")); !os.IsNotExist(err) {
		t.Fatalf("expected no replica db, got %#v", err)
	}

	// Recreate the database and ensure it replicates.
	db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
	if _, err := db0.Exec(`CREATE TABLE t2 (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t2 VALUES (200)`); err != nil {
		t.Fatal(err)
	} else if err := db0.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	db1 = testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	if err := db1.QueryRow(`SELECT x FROM t2`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
	if err := db1.Close(); err != nil {
		t.Fatal(err)
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

func TestMultiNode_DropDB(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	} else if err := db0.Close(); err != nil {
		t.Fatal(err)
	}

	// Ensure the replica has received the database.
	waitForSync(t, "db", cmd0, cmd1)

	// Verify database exists on replica.
	if _, err := os.Stat(filepath.Join(cmd1.Config.FUSE.Dir, "db")); err != nil {
		t.Fatal(err)
	}

	// Remove the database from the primary.
	t.Log("removing database file from primary")
	if err := os.Remove(filepath.Join(cmd0.Config.FUSE.Dir, "db")); err != nil {
		t.Fatal(err)
	}
	t.Log("database file removed")

	// No way to sync if we don't have a database so just wait.
	time.Sleep(1 * time.Second)

	// Verify replica database does not exist.
	if _, err := os.Stat(filepath.Join(cmd1.Config.FUSE.Dir, "db")); !os.IsNotExist(err) {
		t.Fatalf("expected deleted database on replica, got %v", err)
	}

	// Recreate database on primary and ensure it is created on replica.
	t.Log("recreating database file on primary")
	db0 = testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
	if _, err := db0.Exec(`CREATE TABLE t1 (y)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t1 VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	waitForSync(t, "db", cmd0, cmd1)

	// Wait for it to sync and verify on replica.
	var y int
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	if err := db1.QueryRow(`SELECT y FROM t1`).Scan(&y); err != nil {
		t.Fatal(err)
	} else if got, want := y, 200; got != want {
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
		t.Logf("CMD[%d]: %s (%v)", i, litefs.FormatNodeID(cmds[i].Store.ID()), cmds[i].Store.IsPrimary())
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

// Ensure the primary can propagate its high-water mark to the replica nodes.
//
// Currently, the HWM frame is sent immediately after every LTX frame but that
// may change in the future.
func TestMultiNode_HWM(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}

	// Update HWM and write data.
	cmd0.Store.DB("db").SetHWM(1)
	if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)
	time.Sleep(time.Second)
	if got, want := cmd1.Store.DB("db").HWM(), ltx.TXID(1); got != want {
		t.Fatalf("HWM=%s, want %s", got, want)
	}

	// Try it again just for fun.
	cmd0.Store.DB("db").SetHWM(3)
	if _, err := db0.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)
	time.Sleep(time.Second)
	if got, want := cmd1.Store.DB("db").HWM(), ltx.TXID(3); got != want {
		t.Fatalf("HWM=%s, want %s", got, want)
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
	if testingutil.IsWALMode() {
		t.Skip("replicas forward writes in wal mode, skipping")
	}

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

func TestMultiNode_ErrApplyLTX(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)

	mos := mock.NewOS()
	cmd1 := newMountCommand(t, t.TempDir(), cmd0)
	cmd1.OS = mos
	runMountCommand(t, cmd1)
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	ch := make(chan int)
	cmd1.Store.Exit = func(code int) {
		ch <- code
	}
	mos.OpenFileFunc = func(op, name string, flag int, perm fs.FileMode) (*os.File, error) {
		if op == "UPDATESHM" {
			return nil, fmt.Errorf("marker")
		}
		return os.OpenFile(name, flag, perm)
	}

	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}

	select {
	case exitCode := <-ch:
		if got, want := exitCode, 99; got != want {
			t.Fatalf("code=%v, want %v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for exit code")
	}
}

func TestMultiNode_Halt(t *testing.T) {
	t.Run("Commit", func(t *testing.T) {
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

		if _, err := db0.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			t.Fatal(err)
		}

		waitForSync(t, "db", cmd0, cmd1)
		t.Logf("initializing replica client")
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))

		// Acquire the halt lock.
		haltLock, err := cmd1.Store.DB("db").AcquireRemoteHaltLock(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}

		// Write next transaction to the replica.
		t.Logf("inserting value from replica")
		if _, err := db1.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}

		if err := cmd1.Store.DB("db").ReleaseRemoteHaltLock(context.Background(), haltLock.ID); err != nil {
			t.Fatal(err)
		}

		// Verify replica has been updated.
		t.Logf("reading value from replica")
		var sum int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 300; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}

		// Verify primary has been updated.
		t.Logf("reading value from primary")
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 300; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
		t.Logf("done")
	})

	// Ensure that closing a file handle with a halt lock will unhalt the node.
	t.Run("ImplicitUnhalt", func(t *testing.T) {
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

		if _, err := db0.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			t.Fatal(err)
		}

		waitForSync(t, "db", cmd0, cmd1)
		t.Logf("initializing replica client")
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))

		// Acquire the halt lock from the replica via FUSE.
		lockFile, err := os.OpenFile(filepath.Join(cmd1.Config.FUSE.Dir, "db-lock"), os.O_RDWR, 0o666)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = lockFile.Close() }()

		if err := syscall.FcntlFlock(lockFile.Fd(), unix.F_OFD_SETLKW, &syscall.Flock_t{
			Type:  syscall.F_WRLCK,
			Start: int64(litefs.LockTypeHalt),
			Len:   1,
		}); err != nil {
			t.Fatalf("cannot acquire HALT lock: %s", err)
		}

		// Write next transaction to the replica.
		t.Logf("inserting value from replica")
		if _, err := db1.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}

		// Release lock by closing file instead of F_UNLCK.
		if err := lockFile.Close(); err != nil {
			t.Fatal(err)
		}

		// Ensure primary is writeable again.
		t.Logf("inserting value from primary")
		if _, err := db0.Exec(`INSERT INTO t VALUES (300)`); err != nil {
			t.Fatal(err)
		}
		waitForSync(t, "db", cmd0, cmd1)

		// Verify replica has been updated.
		t.Logf("reading value from replica")
		var sum int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 600; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}

		// Verify primary has been updated.
		t.Logf("reading value from primary")
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 600; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
		t.Logf("done")
	})

	t.Run("Rollback", func(t *testing.T) {
		cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
		waitForPrimary(t, cmd0)
		cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))

		// Create a simple table with a single value.
		if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}

		// Ensure we can also write to the replica.
		waitForSync(t, "db", cmd0, cmd1)

		// Acquire the halt lock.
		haltLock, err := cmd1.Store.DB("db").AcquireRemoteHaltLock(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}

		t.Logf("beginning replica transaction")
		tx, err := db1.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		t.Logf("issuing insert from replica")
		if _, err := tx.Exec(`INSERT INTO t VALUES (200)`); err != nil {
			t.Fatal(err)
		}
		t.Logf("rolling back from replica")
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}
		t.Logf("rollback complete")

		if err := cmd1.Store.DB("db").ReleaseRemoteHaltLock(context.Background(), haltLock.ID); err != nil {
			t.Fatal(err)
		}

		// Verify replica.
		var sum int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 100; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}

		// Verify primary.
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 100; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
	})

	t.Run("RollbackWithWrittenWAL", func(t *testing.T) {
		cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
		waitForPrimary(t, cmd0)
		cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
		db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

		// Create a simple table with a single value.
		if _, err := db0.Exec(`CREATE TABLE t (x, y)`); err != nil {
			t.Fatal(err)
		} else if _, err := db0.Exec(`INSERT INTO t VALUES (100, NULL)`); err != nil {
			t.Fatal(err)
		}

		if _, err := db0.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
			t.Fatal(err)
		}

		waitForSync(t, "db", cmd0, cmd1)
		t.Logf("initializing replica client")
		db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
		if _, err := db1.Exec(`PRAGMA cache_size = 2`); err != nil {
			t.Fatal(err)
		}

		// Acquire the halt lock.
		haltLock, err := cmd1.Store.DB("db").AcquireRemoteHaltLock(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}

		// Write next transaction to the replica.
		t.Logf("inserting value from replica")
		tx, err := db1.Begin()
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = tx.Rollback() }()

		// Write enough pages that they flush to the WAL.
		for i := 0; i < 10; i++ {
			if _, err := tx.Exec(`INSERT INTO t VALUES (?, ?)`, i, strings.Repeat("x", 2000)); err != nil {
				t.Fatal(err)
			}
		}

		// Rollback transaction.
		if err := tx.Rollback(); err != nil {
			t.Fatal(err)
		}

		if err := cmd1.Store.DB("db").ReleaseRemoteHaltLock(context.Background(), haltLock.ID); err != nil {
			t.Fatal(err)
		}

		// Write a new transactions over the rolled back one.
		if _, err := db0.Exec(`INSERT INTO t VALUES (200, NULL)`); err != nil {
			t.Fatal(err)
		}

		// Reacquire halt & insert again.
		haltLock, err = cmd1.Store.DB("db").AcquireRemoteHaltLock(context.Background(), 1000)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := db1.Exec(`INSERT INTO t VALUES (300, NULL)`); err != nil {
			t.Fatal(err)
		}
		if err := cmd1.Store.DB("db").ReleaseRemoteHaltLock(context.Background(), haltLock.ID); err != nil {
			t.Fatal(err)
		}

		// Verify replica has been updated.
		t.Logf("reading value from replica")
		var sum int
		if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 600; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}

		// Verify primary has been updated.
		t.Logf("reading value from primary")
		if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
			t.Fatal(err)
		} else if got, want := sum, 600; got != want {
			t.Fatalf("sum=%d, want %d", got, want)
		}
	})
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

func TestMultiNode_Handoff(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmd0, cmd1)

	// Request handoff to the second node.
	if err := cmd0.Store.Handoff(context.Background(), cmd1.Store.ID()); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1 * time.Second)

	// Ensure we can update the new primary node.
	if _, err := db1.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Ensure the data exists on both nodes.
	waitForSync(t, "db", cmd0, cmd1)
	var sum int
	if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
		t.Fatal(err)
	} else if got, want := sum, 300; got != want {
		t.Fatalf("sum=%d, want %d", got, want)
	}

	if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
		t.Fatal(err)
	} else if got, want := sum, 300; got != want {
		t.Fatalf("sum=%d, want %d", got, want)
	}
}

func TestMultiNode_Autopromotion(t *testing.T) {
	cmd0 := newMountCommand(t, t.TempDir(), nil)
	runMountCommand(t, cmd0)
	waitForPrimary(t, cmd0)

	// Create a simple table with a single value.
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Start a new instance with autopromotion enabled.
	cmd1 := newMountCommand(t, t.TempDir(), cmd0)
	cmd1.Config.Lease.Promote = true
	runMountCommand(t, cmd1)
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))

	// Ensure we can update the new primary node.
	if _, err := db1.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Ensure the data exists on both nodes.
	waitForSync(t, "db", cmd0, cmd1)
	var sum int
	if err := db1.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
		t.Fatal(err)
	} else if got, want := sum, 300; got != want {
		t.Fatalf("sum=%d, want %d", got, want)
	}

	if err := db0.QueryRow(`SELECT SUM(x) FROM t`).Scan(&sum); err != nil {
		t.Fatal(err)
	} else if got, want := sum, 300; got != want {
		t.Fatalf("sum=%d, want %d", got, want)
	}
}

func TestMultiNode_DatabaseFilter(t *testing.T) {
	cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
	waitForPrimary(t, cmd0)
	cmd1 := newMountCommand(t, t.TempDir(), cmd0)
	cmd1.OnInitStore = func() {
		cmd1.Store.DatabaseFilter = []string{"x.db"}
	}
	runMountCommand(t, cmd1)

	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "x.db"))
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if err := db0.Close(); err != nil {
		t.Fatal(err)
	}

	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "y.db"))
	if _, err := db1.Exec(`CREATE TABLE t (y)`); err != nil {
		t.Fatal(err)
	} else if err := db1.Close(); err != nil {
		t.Fatal(err)
	}

	waitForSync(t, "x.db", cmd0, cmd1)

	// Only the filtered database should exist.
	if _, err := os.Stat(filepath.Join(cmd1.Config.FUSE.Dir, "x.db")); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(cmd1.Config.FUSE.Dir, "y.db")); !os.IsNotExist(err) {
		t.Fatal("expected second database to not exist on replica")
	}
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
	// Ensure files can be removed when they are older than the retention period.
	t.Run("Expiry", func(t *testing.T) {
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
	})

	// Ensure files can be removed when they are beyond the high-water mark.
	t.Run("HWM", func(t *testing.T) {
		cmd := newMountCommand(t, t.TempDir(), nil)
		cmd.Config.Data.Retention = 1 * time.Millisecond
		cmd.Config.Data.RetentionMonitorInterval = 0
		waitForPrimary(t, runMountCommand(t, cmd))

		// Add backup client after it is initialized. File-based backups are
		// only available for testing purposes.
		cmd.Store.BackupClient = litefs.NewFileBackupClient(t.TempDir())

		// Create multiple transactions.
		db := testingutil.OpenSQLDB(t, filepath.Join(cmd.Config.FUSE.Dir, "db"))
		txID := cmd.Store.DB("db").TXID() // normalize for journal mode
		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < 9-int(txID); i++ {
			if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
				t.Fatal(err)
			}
		}

		// Wait for retention period.
		time.Sleep(cmd.Config.Data.Retention)

		// Run enforcement and ensure all files are still available.
		if err := cmd.Store.EnforceRetention(context.Background()); err != nil {
			t.Fatal(err)
		}
		if ents, err := cmd.Store.DB("db").ReadLTXDir(); err != nil {
			t.Fatal(err)
		} else if got, want := len(ents), 10; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}

		// Move HWM forward and rerun enforcement.
		cmd.Store.DB("db").SetHWM(5)
		if err := cmd.Store.EnforceRetention(context.Background()); err != nil {
			t.Fatal(err)
		}
		if ents, err := cmd.Store.DB("db").ReadLTXDir(); err != nil {
			t.Fatal(err)
		} else if got, want := len(ents), 6; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}

		// Move HWM forward to latest and recheck.
		cmd.Store.DB("db").SetHWM(10)
		if err := cmd.Store.EnforceRetention(context.Background()); err != nil {
			t.Fatal(err)
		}
		if ents, err := cmd.Store.DB("db").ReadLTXDir(); err != nil {
			t.Fatal(err)
		} else if got, want := len(ents), 1; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}

		// Move HWM past latest and ensure last file is always retained.
		cmd.Store.DB("db").SetHWM(1000)
		if err := cmd.Store.EnforceRetention(context.Background()); err != nil {
			t.Fatal(err)
		}
		if ents, err := cmd.Store.DB("db").ReadLTXDir(); err != nil {
			t.Fatal(err)
		} else if got, want := len(ents), 1; got != want {
			t.Fatalf("n=%d, want %d", got, want)
		}
	})
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

// Ensure that a node from an existing cluster cannot accidentally rejoined another cluster.
func TestMultiNode_ClusterIDMismatch(t *testing.T) {
	dir0, dir1 := t.TempDir(), t.TempDir()

	cmd0 := runMountCommand(t, newMountCommand(t, dir0, nil))
	waitForPrimary(t, cmd0)
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

	// Create a simple table with a single value on one cluster.
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db0.Exec(`INSERT INTO t VALUES (100)`); err != nil {
		t.Fatal(err)
	}

	// Create a simple table with a single value on the second cluster.
	cmd1 := runMountCommand(t, newMountCommand(t, dir1, nil))
	db1 := testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	if _, err := db1.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	} else if _, err := db1.Exec(`INSERT INTO t VALUES (200)`); err != nil {
		t.Fatal(err)
	}

	// Close secondary cluster.
	if err := db1.Close(); err != nil {
		t.Fatal(err)
	} else if err := cmd1.Close(); err != nil {
		t.Fatal(err)
	}

	// Restart secondary cluster node but connect to the first cluster.
	cmd1 = newMountCommand(t, dir1, cmd0)
	cmd1.Config.SkipSync = true // don't wait for sync since it shouldn't happen
	runMountCommand(t, cmd1)

	// Ensure node does not become ready.
	select {
	case <-cmd1.Store.ReadyCh():
		t.Fatal("node should not become ready")
	case <-time.After(time.Second):
	}

	// Verify that data hasn't changed on second node.
	db1 = testingutil.OpenSQLDB(t, filepath.Join(cmd1.Config.FUSE.Dir, "db"))
	var x int
	if err := db1.QueryRow(`SELECT x FROM t`).Scan(&x); err != nil {
		t.Fatal(err)
	} else if got, want := x, 200; got != want {
		t.Fatalf("x=%d, want %d", got, want)
	}
}

// See: https://github.com/superfly/litefs/issues/339
func TestMultiNode_WriteSnapshot_LockingProtocol(t *testing.T) {
	t.Skip("This test takes a long time. Run it manually if you need to simulate a large database snapshot.")

	// Create a proxy so we can slow down replication.
	var primaryPort int
	receiving := make(chan struct{})
	finished := make(chan struct{})
	s := httptest.NewServer(h2c.NewHandler(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/stream" {
			t.Fatalf("unexpected proxy URL path: %s", r.URL.Path)
		}

		// Read request body.
		reqBody, err := io.ReadAll(r.Body)
		if err != nil {
			t.Fatal(err)
		}

		u := r.URL
		u.Scheme = "http"
		u.Host = fmt.Sprintf("localhost:%d", primaryPort)

		req, err := http.NewRequest(r.Method, u.String(), bytes.NewReader(reqBody))
		if err != nil {
			t.Fatal(err)
		}
		resp, err := HTTPClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		t.Logf("snapshot begun, blocking proxy")
		receiving <- struct{}{}

		// Wait until we get notified to finish the snapshot.
		select {
		case <-r.Context().Done():
			return
		case <-finished:
			t.Logf("notified of test completion, stopping proxy early")
		}
	}), &http2.Server{}))
	t.Logf("proxy running on: %s", s.URL)

	// Start the primary node.
	cmd0 := newMountCommand(t, t.TempDir(), nil)
	cmd0.AdvertiseURLFn = nil              // clear helper function for advertise-url
	cmd0.Config.Lease.AdvertiseURL = s.URL // advertise the proxy's address.
	runMountCommand(t, cmd0)
	waitForPrimary(t, cmd0)
	primaryPort = cmd0.HTTPServer.Port()

	// Create a database that is large enough that we can avoid buffering it all.
	t.Logf("building large database")
	db0 := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))
	if _, err := db0.Exec(`CREATE TABLE t (x)`); err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := db0.Exec(`INSERT INTO t VALUES (?)`, strings.Repeat("x", 1<<26)); err != nil {
			t.Fatal(err)
		}
	}
	if err := db0.Close(); err != nil {
		t.Fatal(err)
	}
	t.Logf("database complete")

	// Connect a replica but receive the snapshot slowly.
	cmd1 := newMountCommand(t, t.TempDir(), cmd0)
	cmd1.Config.Lease.ReconnectDelay = 1 * time.Second
	go func() {
		// Wait until we start receiving the snapshot.
		select {
		case <-time.After(10 * time.Second):
			panic("did not start receiving snapshot")
		case <-receiving:
			t.Logf("successfully started snapshot, attempting ready of primary database")
		}

		for i := 0; i < 10; i++ {
			db, err := sql.Open("sqlite3", filepath.Join(cmd0.Config.FUSE.Dir, "db"))
			if err != nil {
				panic(err)
			}
			defer func() { _ = db.Close() }()

			// Attempt to query primary database.
			var count int
			t.Logf("attempting to query primary")
			if err := db.QueryRow(`SELECT COUNT(*) FROM t`).Scan(&count); err != nil {
				panic(err)
			}
			t.Logf("successfully queried primary: n=%d", count)

			// Attempt to write to primary database.
			if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
				panic(err)
			}
			t.Logf("successfully mutated primary")

			if err := db.Close(); err != nil {
				panic(err)
			}
		}

		// Allow snapshot to finish.
		finished <- struct{}{}
		_ = cmd1.Close()
	}()

	// Run replica mount and wait until ready.
	runMountCommand(t, cmd1)
}

func TestEventStream(t *testing.T) {
	t.Run("Tx/Primary", func(t *testing.T) {
		cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
		db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

		resp, err := http.Get(cmd0.HTTPServer.URL() + "/events")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		dec := json.NewDecoder(resp.Body)

		var offset ltx.TXID
		if testingutil.IsWALMode() {
			offset = 1
		}

		var event litefs.Event
		if err := dec.Decode(&event); err != nil {
			t.Fatal(err)
		} else if got, want := event.Type, "init"; got != want {
			t.Fatalf("type=%s, want %s", got, want)
		}

		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := dec.Decode(&event); err != nil {
			t.Fatal(err)
		} else if got, want := event.Type, "tx"; got != want {
			t.Fatalf("type=%s, want %s", got, want)
		} else if got, want := event.DB, "db"; got != want {
			t.Fatalf("db=%s, want %s", got, want)
		} else if got, want := event.Data.(*litefs.TxEventData).TXID, ltx.TXID(offset+1); got != want {
			t.Fatalf("data.txid=%s, want %s", got, want)
		}

		if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		if err := dec.Decode(&event); err != nil {
			t.Fatal(err)
		} else if got, want := event.Type, "tx"; got != want {
			t.Fatalf("type=%s, want %s", got, want)
		} else if got, want := event.DB, "db"; got != want {
			t.Fatalf("db=%s, want %s", got, want)
		} else if got, want := event.Data.(*litefs.TxEventData).TXID, ltx.TXID(offset+2); got != want {
			t.Fatalf("data.txid=%s, want %s", got, want)
		}
	})

	t.Run("Tx/Replica", func(t *testing.T) {
		cmd0 := runMountCommand(t, newMountCommand(t, t.TempDir(), nil))
		waitForPrimary(t, cmd0)
		cmd1 := runMountCommand(t, newMountCommand(t, t.TempDir(), cmd0))
		db := testingutil.OpenSQLDB(t, filepath.Join(cmd0.Config.FUSE.Dir, "db"))

		resp, err := http.Get(cmd1.HTTPServer.URL() + "/events")
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = resp.Body.Close() }()

		dec := json.NewDecoder(resp.Body)

		var event litefs.Event
		if err := dec.Decode(&event); err != nil {
			t.Fatal(err)
		} else if got, want := event.Type, "init"; got != want {
			t.Fatalf("type=%s, want %s", got, want)
		}

		var offset ltx.TXID
		if testingutil.IsWALMode() {
			offset = 1

			if err := dec.Decode(&event); err != nil {
				t.Fatal(err)
			} else if got, want := event.Type, "tx"; got != want {
				t.Fatalf("type=%s, want %s", got, want)
			} else if got, want := event.DB, "db"; got != want {
				t.Fatalf("db=%s, want %s", got, want)
			} else if got, want := event.Data.(*litefs.TxEventData).TXID, ltx.TXID(1); got != want {
				t.Fatalf("data.txid=%s, want %s", got, want)
			}
		}

		if _, err := db.Exec(`CREATE TABLE t (x)`); err != nil {
			t.Fatal(err)
		}
		if err := dec.Decode(&event); err != nil {
			t.Fatal(err)
		} else if got, want := event.Type, "tx"; got != want {
			t.Fatalf("type=%s, want %s", got, want)
		} else if got, want := event.DB, "db"; got != want {
			t.Fatalf("db=%s, want %s", got, want)
		} else if got, want := event.Data.(*litefs.TxEventData).TXID, ltx.TXID(offset+1); got != want {
			t.Fatalf("data.txid=%s, want %s", got, want)
		}

		if _, err := db.Exec(`INSERT INTO t VALUES (100)`); err != nil {
			t.Fatal(err)
		}
		if err := dec.Decode(&event); err != nil {
			t.Fatal(err)
		} else if got, want := event.Type, "tx"; got != want {
			t.Fatalf("type=%s, want %s", got, want)
		} else if got, want := event.DB, "db"; got != want {
			t.Fatalf("db=%s, want %s", got, want)
		} else if got, want := event.Data.(*litefs.TxEventData).TXID, ltx.TXID(offset+2); got != want {
			t.Fatalf("data.txid=%s, want %s", got, want)
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
	if _, err := db.Exec(`CREATE TABLE t (id INTEGER PRIMARY KEY AUTOINCREMENT, node_id INTEGER, value TEXT)`); err != nil {
		t.Fatal(err)
	} else if err := db.Close(); err != nil {
		t.Fatal(err)
	}
	waitForSync(t, "db", cmds...)

	// Continually run queries against nodes.
	var wg sync.WaitGroup
	for i, m := range cmds {
		i, m := i, m

		wg.Add(1)
		func() {
			defer wg.Done()

			db := testingutil.OpenSQLDB(t, filepath.Join(m.Config.FUSE.Dir, "db"))

			// Open lock file handle so we can HALT lock.
			lockFile, err := os.OpenFile(filepath.Join(m.Config.FUSE.Dir, "db-lock"), os.O_RDWR, 0o666)
			if err != nil {
				t.Errorf("open database file handle: %s", err)
				return
			}
			defer func() { _ = lockFile.Close() }()

			ticker := time.NewTicker(100 * time.Millisecond)
			defer ticker.Stop()

			for j := 0; ; j++ {
				select {
				case <-done:
					return // test time has elapsed
				case <-ticker.C:
					// Acquire HALT lock through FUSE.
					if err := syscall.FcntlFlock(lockFile.Fd(), unix.F_OFD_SETLKW, &syscall.Flock_t{
						Type:  syscall.F_WRLCK,
						Start: int64(litefs.LockTypeHalt),
						Len:   1,
					}); err != nil {
						t.Errorf("cannot acquire HALT lock: %s", err)
						return
					}

					t.Logf("@%s: insert", m.Store.DB("db").Pos())
					if _, err := db.Exec(`INSERT INTO t (node_id, value) VALUES (?, ?)`, i, strings.Repeat("x", 200)); err != nil {
						t.Errorf("cannot insert (node %d, iter %d): %s", i, j, err)
						return
					}

					// Release HALT lock through FUSE.
					if err := syscall.FcntlFlock(lockFile.Fd(), unix.F_OFD_SETLKW, &syscall.Flock_t{
						Type:  syscall.F_UNLCK,
						Start: int64(litefs.LockTypeHalt),
						Len:   1,
					}); err != nil {
						t.Errorf("cannot release HALT lock: %s", err)
						return
					}

					<-ticker.C

					t.Logf("@%s: read", m.Store.DB("db").Pos())

					var id, nodeID int
					var value string
					if err := db.QueryRow(`SELECT id, node_id, value FROM t ORDER BY id DESC LIMIT 1`).Scan(&id, &nodeID, &value); err != nil && err != sql.ErrNoRows {
						t.Errorf("cannot query (node %d, iter %d): %s", i, j, err)
						return
					}
				}
			}
		}()
	}

	// Ensure we have the same data once we resync.
	wg.Wait()
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
		cmd.Config.FUSE.Dir = ""
		if err := cmd.Validate(context.Background()); err == nil || err.Error() != `fuse directory required` {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("ErrDataDirectoryRequired", func(t *testing.T) {
		cmd := main.NewMountCommand()
		cmd.Config.FUSE.Dir = t.TempDir()
		cmd.Config.Data.Dir = ""
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
		if err := cmd.Close(); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
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

// waitForBackupSync waits for the backup state to match the cmd.
func waitForBackupSync(tb testing.TB, cmd *main.MountCommand) {
	tb.Helper()

	testingutil.RetryUntil(tb, 1*time.Millisecond, 5*time.Second, func() error {
		tb.Helper()

		localPosMap := cmd.Store.PosMap()
		backupPosMap, err := cmd.Store.BackupClient.PosMap(context.Background())
		if err != nil {
			return err
		}

		if !reflect.DeepEqual(localPosMap, backupPosMap) {
			return fmt.Errorf("waiting for backup sync: local=%#v, backup=%#v", localPosMap, backupPosMap)
		}

		slog.Debug("command synced with backup", slog.Any("pos", localPosMap))

		return nil
	})
}

var HTTPClient = &http.Client{
	Transport: &http2.Transport{
		AllowHTTP: true,
		DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
			return net.Dial(network, addr) // h2c-only right now
		},
	},
}
