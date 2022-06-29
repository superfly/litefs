package main_test

import (
	"context"
	"flag"
	"log"
	"os"
	"path/filepath"
	"testing"

	main "github.com/superfly/litefs/cmd/litefs"
	"github.com/superfly/litefs/internal/testingutil"
)

var debug = flag.Bool("debug", false, "enable fuse debugging")

func init() {
	log.SetFlags(0)
}

func TestLocalOnly(t *testing.T) {
	m0 := newRunningMain(t)
	db := testingutil.OpenSQLDB(t, filepath.Join(m0.Store.Path(), "db"))

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

func newMain(tb testing.TB) *main.Main {
	tb.Helper()

	dir := tb.TempDir()
	tb.Cleanup(func() {
		if err := os.RemoveAll(dir); err != nil {
			tb.Fatalf("cannot remove temp directory: %s", err)
		}
	})

	m := main.NewMain()
	m.MountDir = dir
	m.Addr = ":0"
	m.ConsulURL = "http://localhost:8500"
	m.Debug = *debug

	return m
}

func newRunningMain(tb testing.TB) *main.Main {
	tb.Helper()

	m := newMain(tb)
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
