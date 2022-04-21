package main

import (
	"context"
	"database/sql"
	"embed"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

//go:embed index.html
var embedFS embed.FS

var (
	dsn    = flag.String("dsn", "", "datasource name")
	addr   = flag.String("addr", ":8080", "bind address")
	region = flag.String("region", os.Getenv("FLY_REGION"), "fly.io region")
)

var notify struct {
	mu sync.Mutex
	ch chan struct{}

	value  int
	region string
}

func main() {
	if err := run(context.Background()); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}

func run(ctx context.Context) error {
	flag.Parse()
	if *dsn == "" {
		return fmt.Errorf("flag required: -dsn DSN")
	}
	if *region == "" {
		*region = "ord"
	}

	notify.ch = make(chan struct{})

	log.Printf("region: %s", os.Getenv("FLY_REGION"))
	log.Printf("opening database: %s", *dsn)

	db, err := sql.Open("sqlite3", *dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec(`CREATE TABLE IF NOT EXISTS t (id INTEGER PRIMARY KEY, value INTEGER, region TEXT)`); err != nil {
		return fmt.Errorf("create table: %w", err)
	} else if _, err := db.Exec(`INSERT INTO t (id, value, region) VALUES (1, 0, ?) ON CONFLICT (id) DO NOTHING`, *region); err != nil {
		return fmt.Errorf("insert initial record: %w", err)
	} else if err := db.Close(); err != nil {
		return fmt.Errorf("close database: %w", err)
	}

	go func() {
		if err := monitor(ctx); err != nil {
			log.Fatalf("watcher: %s", err)
		}
	}()

	log.Printf("listening on %s", *addr)

	return http.ListenAndServe(*addr, &handler{})
}

// monitor runs in a separate goroutine and monitors the main DB & WAL file.
func monitor(ctx context.Context) error {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := checkDB(ctx); err != nil {
				log.Printf("check db: %s", err)
			}
		}
	}
}

func checkDB(ctx context.Context) error {
	notify.mu.Lock()
	defer notify.mu.Unlock()

	db, err := sql.Open("sqlite3", *dsn)
	if err != nil {
		return fmt.Errorf("open database: %w", err)
	}
	defer db.Close()

	var value int
	var region string
	if err := db.QueryRowContext(ctx, `SELECT value, region FROM t WHERE id = 1`).Scan(&value, &region); err != nil {
		return fmt.Errorf("query: %w", err)
	}

	// Ignore if the value is the same.
	if notify.value == value {
		return nil
	}

	notify.value = value
	notify.region = region
	log.Printf("update: value=%d region=%s", notify.value, notify.region)

	// Notify watchers.
	close(notify.ch)
	notify.ch = make(chan struct{})

	return nil
}

type handler struct{}

func (h *handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// All API calls must have a "region" specified.
	if strings.HasPrefix(r.URL.Path, "/api") {
		if v := r.URL.Query().Get("region"); v == "" {
			httpError(w, r, "Region required", http.StatusBadRequest)
			return
		} else if v != *region {
			log.Printf("redirecting stream from %q to %q", *region, v)
			w.Header().Set("fly-replay", "region="+v)
			return
		}

		log.Printf("http: %s %s", r.Method, &url.URL{Path: r.URL.Path, RawQuery: r.URL.RawQuery})
	}

	switch r.URL.Path {
	case "/api/inc":
		h.handleInc(w, r)
	case "/api/stream":
		h.handleStream(w, r)
	default:
		http.FileServer(http.FS(embedFS)).ServeHTTP(w, r)
	}
}

func (h *handler) handleInc(w http.ResponseWriter, r *http.Request) {
	if r.Method != "POST" {
		httpError(w, r, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	db, err := sql.Open("sqlite3", *dsn)
	if err != nil {
		httpError(w, r, "open db: "+err.Error(), http.StatusInternalServerError)
		return
	}
	defer db.Close()

	if _, err := db.ExecContext(r.Context(), `UPDATE t SET value = value + 1, region = ? WHERE id = 1`, *region); err != nil {
		httpError(w, r, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Write([]byte(`{}`))
}

func (h *handler) handleStream(w http.ResponseWriter, r *http.Request) {
	if r.Method != "GET" {
		httpError(w, r, "Method not allowed", http.StatusMethodNotAllowed)
		return
	}

	log.Printf("stream connected %p", r)
	defer log.Printf("stream disconnected %p", r)

	w.Header().Set("Cache-Control", "no-store")
	w.Header().Set("Content-Type", "text/event-stream")

	notify.mu.Lock()
	notifyCh, value, rgn := notify.ch, notify.value, notify.region
	notify.mu.Unlock()

	for {
		// Marshal data & write SSE message.
		if buf, err := json.Marshal(Event{Value: value, Region: rgn}); err != nil {
			log.Printf("cannot marshal event: %s", err)
			return
		} else if _, err := fmt.Fprintf(w, "event: update\ndata: %s\n\n", buf); err != nil {
			log.Printf("cannot write update event: %s", err)
			return
		}
		w.(http.Flusher).Flush()

		// Wait for change to value.
		select {
		case <-r.Context().Done():
			return
		case <-notifyCh:
		}

		notify.mu.Lock()
		notifyCh, value, rgn = notify.ch, notify.value, notify.region
		notify.mu.Unlock()
	}
}

type Event struct {
	Value  int    `json:"value"`
	Region string `json:"region"`
}

func httpError(w http.ResponseWriter, r *http.Request, err string, status int) {
	log.Printf("http error: %s %s: %s", r.Method, r.URL, err)

	w.Header().Set("Content-type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(struct {
		Error string `json:"error"`
	}{err})
}
