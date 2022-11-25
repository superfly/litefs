package main

import (
	"context"
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

var (
	journalMode    = flag.String("journal-mode", "", "journal mode")
	seed           = flag.Int64("seed", 0, "prng seed")
	cacheSize      = flag.Int("cache-size", -2000, "SQLite cache size")
	iter           = flag.Int("iter", 0, "number of iterations")
	maxRowSize     = flag.Int("max-row-size", 256, "maximum row size")
	maxRowsPerIter = flag.Int("max-rows-per-iter", 1000, "maximum number of rows per iteration")
)

func main() {
	flag.Usage = Usage

	if err := run(context.Background()); err == flag.ErrHelp {
		os.Exit(1)
	} else if err != nil {
		fmt.Fprintf(os.Stderr, "ERROR: %s\n", err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	flag.Parse()
	if flag.NArg() == 0 {
		flag.Usage()
		return flag.ErrHelp
	} else if flag.NArg() > 1 {
		return fmt.Errorf("too many arguments")
	}

	// Initialize PRNG.
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	fmt.Printf("running litefs-bench: seed=%d\n", seed)
	rand.Seed(*seed)

	// Open database.
	dsn := flag.Arg(0)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	// Initialize cache.
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA cache_size = %d`, cacheSize)); err != nil {
		return fmt.Errorf("set cache size to %d: %w", *cacheSize, err)
	}

	// Set journal mode, if set. Otherwise defaults to "DELETE" for new databases.
	if *journalMode != "" {
		if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, num INTEGER, data TEXT)`); err != nil {
			return fmt.Errorf("create table: %w", err)
		}
	}

	// Run migrations, if necessary.
	if err := migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// Begin monitoring stats.
	go monitor(ctx)

	// Execute once for each iteration.
	for i := 0; *iter == 0 || i < *iter; i++ {
		if err := runIter(ctx, db); err != nil {
			return fmt.Errorf("iter %d: %w", i, err)
		}
	}

	if err := db.Close(); err != nil {
		return err
	}

	return nil
}

func migrate(ctx context.Context, db *sql.DB) error {
	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, num INTEGER, data TEXT)`); err != nil {
		return fmt.Errorf("create table: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx ON t (num)`); err != nil {
		return fmt.Errorf("create index: %w", err)
	}
	return nil
}

// runIter runs a single iteration of the benchmark.
func runIter(ctx context.Context, db *sql.DB) error {
	buf := make([]byte, *maxRowSize)

	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	rowN := rand.Intn(*maxRowsPerIter) + 1
	for i := 0; i < rowN; i++ {
		_, _ = rand.Read(buf)
		num := rand.Int63()
		rowSize := rand.Intn(*maxRowSize)
		data := fmt.Sprintf("%x", buf)[:rowSize]

		if _, err := tx.Exec(`INSERT INTO t (num, data) VALUES (?, ?)`, num, data); err != nil {
			return fmt.Errorf("insert(%d): %w", i, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit: %w", err)
	}

	// Update stats on success.
	statsMu.Lock()
	defer statsMu.Unlock()
	stats.TxN++
	stats.RowN += rowN

	return nil
}

// monitor periodically prints stats.
func monitor(ctx context.Context) {
	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	prevTime := time.Now()
	var prev Stats
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			statsMu.Lock()
			curr := stats
			statsMu.Unlock()

			currTime := time.Now()
			elapsed := currTime.Sub(prevTime).Seconds()

			log.Printf("stats: tx/sec=%0.03f rows/sec=%0.03f",
				float64(curr.TxN-prev.TxN)/elapsed,
				float64(curr.RowN-prev.RowN)/elapsed,
			)

			prev, prevTime = curr, currTime
		}
	}
}

var statsMu sync.Mutex
var stats Stats

type Stats struct {
	TxN  int
	RowN int
}

func Usage() {
	fmt.Printf(`
litefs-bench is a tool for simulating load against a SQLite database.

Usage:

	litefs-bench [arguments] DSN

Arguments:

`[1:])
	flag.PrintDefaults()
	fmt.Println()
}
