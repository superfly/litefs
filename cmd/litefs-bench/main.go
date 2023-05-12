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
	litefsgo "github.com/superfly/litefs-go"
)

var dsn string

var (
	mode           = flag.String("mode", "", "benchmark mode")
	journalMode    = flag.String("journal-mode", "", "journal mode")
	seed           = flag.Int64("seed", 0, "prng seed")
	cacheSize      = flag.Int("cache-size", -2000, "SQLite cache size")
	iter           = flag.Int("iter", 0, "number of iterations")
	maxRowSize     = flag.Int("max-row-size", 256, "maximum row size")
	maxRowsPerIter = flag.Int("max-rows-per-iter", 10, "maximum number of rows per iteration")
	iterPerSec     = flag.Float64("iter-per-sec", 0, "iterations per second")
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
	} else if *mode == "" {
		return fmt.Errorf("required: -mode MODE")
	}

	// Initialize PRNG.
	if *seed == 0 {
		*seed = time.Now().UnixNano()
	}
	fmt.Printf("running litefs-bench: seed=%d\n", seed)

	// Open database.
	dsn = flag.Arg(0)
	db, err := sql.Open("sqlite3", dsn)
	if err != nil {
		return err
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`PRAGMA busy_timeout = 5000`); err != nil {
		return fmt.Errorf("set busy timeout: %w", err)
	}

	// Initialize cache.
	if _, err := db.Exec(fmt.Sprintf(`PRAGMA cache_size = %d`, cacheSize)); err != nil {
		return fmt.Errorf("set cache size to %d: %w", *cacheSize, err)
	}

	// Set journal mode, if set. Otherwise defaults to "DELETE" for new databases.
	if *journalMode != "" {
		log.Printf("setting journal mode to %q", *journalMode)
		if _, err := db.Exec(`PRAGMA journal_mode = ` + *journalMode); err != nil {
			return fmt.Errorf("set journal mode: %w", err)
		}
	}

	// Run migrations, if necessary.
	if err := migrate(ctx, db); err != nil {
		return fmt.Errorf("migrate: %w", err)
	}

	// Begin monitoring stats.
	go monitor(ctx)

	// Enforce rate limit.
	rate := time.Nanosecond
	if *iterPerSec > 0 {
		rate = time.Duration(float64(time.Second) / *iterPerSec)
	}
	ticker := time.NewTicker(rate)
	defer ticker.Stop()

	// Execute once for each iteration.
	for i := 0; *iter == 0 || i < *iter; i++ {
		rand := rand.New(rand.NewSource(*seed + int64(i)))

		select {
		case <-ctx.Done():
			return context.Cause(ctx)

		case <-ticker.C:
			var err error
			switch *mode {
			case "insert":
				err = runInsertIter(ctx, db, rand)
			case "query":
				err = runQueryIter(ctx, db, rand)
			default:
				return fmt.Errorf("invalid bench mode: %q", *mode)
			}
			if err != nil {
				return fmt.Errorf("iter %d: %w", i, err)
			}
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

// runInsertIter runs a single "insert" iteration.
func runInsertIter(ctx context.Context, db *sql.DB, rand *rand.Rand) error {
	buf := make([]byte, *maxRowSize)

	return litefsgo.WithHalt(dsn, func() error {
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

		// Vacuum periodically.
		if rand.Intn(100) == 0 {
			if _, err := db.Exec(`VACUUM`); err != nil {
				return fmt.Errorf("vacuum: %w", err)
			}
		}

		// Truncate periodically.
		if rand.Intn(10) == 0 {
			if _, err := db.Exec(`PRAGMA wal_checkpoint(TRUNCATE)`); err != nil {
				return fmt.Errorf("truncate: %w", err)
			}
		}
		return err
	})
}

// runQueryIter runs a single "query" iteration.
func runQueryIter(ctx context.Context, db *sql.DB, rand *rand.Rand) error {
	tx, err := db.Begin()
	if err != nil {
		return fmt.Errorf("begin: %w", err)
	}
	defer func() { _ = tx.Rollback() }()

	// Determine highest possible ID.
	var maxID int
	if err := tx.QueryRow(`SELECT MAX(id) FROM t`).Scan(&maxID); err == sql.ErrNoRows {
		log.Printf("no rows available, skipping")
		return nil
	} else if err != nil {
		return fmt.Errorf("query max id: %w", err)
	}

	// Read data starting from a random row.
	rows, err := tx.Query(`SELECT id, num, data FROM t WHERE id >= ?`, maxID)
	if err != nil {
		return fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	rowN := rand.Intn(*maxRowsPerIter) + 1
	for i := 0; rows.Next() && i < rowN; i++ {
		var id, num int
		var data []byte
		if err := rows.Scan(&id, &num, &data); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
	}
	if err := rows.Close(); err != nil {
		return fmt.Errorf("close rows: %w", err)
	}

	if err := tx.Rollback(); err != nil {
		return fmt.Errorf("rollback: %w", err)
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

	litefs-bench MODE [arguments] DSN

Modes:

	insert       continuous INSERTs into a single indexed table
	query        continuous short SELECT queries against a table

Arguments:

`[1:])
	flag.PrintDefaults()
	fmt.Println()
}
