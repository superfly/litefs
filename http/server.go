package http

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal/chunk"
	"github.com/superfly/ltx"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
)

// Default settings
const (
	DefaultAddr = ":20202"
)

var ErrServerClosed = fmt.Errorf("canceled, http server closed")

// Server represents an HTTP API server for LiteFS.
type Server struct {
	ln net.Listener

	httpServer  *http.Server
	http2Server *http2.Server
	promHandler http.Handler

	addr  string
	store *litefs.Store

	g      errgroup.Group
	ctx    context.Context
	cancel context.CancelCauseFunc
}

func NewServer(store *litefs.Store, addr string) *Server {
	s := &Server{
		addr:  addr,
		store: store,
	}
	s.ctx, s.cancel = context.WithCancelCause(context.Background())

	s.promHandler = promhttp.Handler()
	s.http2Server = &http2.Server{}
	s.httpServer = &http.Server{
		Handler: h2c.NewHandler(http.HandlerFunc(s.serveHTTP), s.http2Server),
		BaseContext: func(_ net.Listener) context.Context {
			return s.ctx
		},
	}
	return s
}

func (s *Server) Listen() (err error) {
	if s.ln, err = net.Listen("tcp", s.addr); err != nil {
		return err
	}
	return nil
}

func (s *Server) Serve() {
	s.g.Go(func() error {
		if err := s.httpServer.Serve(s.ln); s.ctx.Err() != nil {
			return err
		}
		return nil
	})
}

func (s *Server) Close() (err error) {
	if s.ln != nil {
		if e := s.ln.Close(); err == nil {
			err = e
		}
	}
	if s.httpServer != nil {
		if e := s.httpServer.Close(); err == nil {
			err = e
		}
	}
	s.cancel(ErrServerClosed)
	if e := s.g.Wait(); e != nil && err == nil {
		err = e
	}
	return err
}

// Port returns the port the listener is running on.
func (s *Server) Port() int {
	if s.ln == nil {
		return 0
	}
	return s.ln.Addr().(*net.TCPAddr).Port
}

// URL returns the full base URL for the running server.
func (s *Server) URL() string {
	host, _, _ := net.SplitHostPort(s.addr)
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, fmt.Sprint(s.Port())))
}

func (s *Server) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if strings.HasPrefix(r.URL.Path, "/debug/pprof") {
		switch r.URL.Path {
		case "/debug/pprof/cmdline":
			pprof.Cmdline(w, r)
		case "/debug/pprof/profile":
			pprof.Profile(w, r)
		case "/debug/pprof/symbol":
			pprof.Symbol(w, r)
		case "/debug/pprof/trace":
			pprof.Trace(w, r)
		default:
			pprof.Index(w, r)
		}
		return
	}

	switch r.URL.Path {
	case "/debug/vars":
		expvar.Handler().ServeHTTP(w, r)
		return
	case "/metrics":
		s.promHandler.ServeHTTP(w, r)
		return
	}

	switch r.URL.Path {
	case "/halt":
		switch r.Method {
		case http.MethodPost:
			s.handlePostHalt(w, r)
		case http.MethodDelete:
			s.handleDeleteHalt(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	case "/tx":
		switch r.Method {
		case http.MethodPost:
			s.handlePostTx(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	case "/import":
		switch r.Method {
		case http.MethodPost:
			s.handlePostImport(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}
	case "/stream":
		switch r.Method {
		case http.MethodPost:
			s.handlePostStream(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handlePostImport(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		Error(w, r, fmt.Errorf("name required"), http.StatusBadRequest)
		return
	}

	// Wrap context so that it cancels when the primary lease is lost.
	r = r.WithContext(s.store.PrimaryCtx(r.Context()))
	if err := r.Context().Err(); err != nil {
		Error(w, r, err, http.StatusServiceUnavailable)
		return
	}

	db, err := s.store.CreateDBIfNotExists(name)
	if err != nil {
		Error(w, r, fmt.Errorf("create database: %w", err), http.StatusInternalServerError)
		return
	}

	if err := db.Import(r.Context(), r.Body); err != nil {
		Error(w, r, err, http.StatusInternalServerError)
		return
	}
}

func (s *Server) handlePostHalt(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")

	// Cannot issue remote halt lock from this node.
	if id := r.Header.Get("Litefs-Id"); id == s.store.ID() {
		Error(w, r, fmt.Errorf("cannot remotely halt self"), http.StatusBadRequest)
		return
	}

	// Ensure database exists before attempting a lock.
	db, err := s.store.CreateDBIfNotExists(name)
	if err != nil {
		Error(w, r, fmt.Errorf("create db: %w", err), http.StatusInternalServerError)
		return
	}

	// Acquire write locks on behalf of remote node.
	haltLock, err := db.AcquireHaltLock(r.Context())
	if err != nil {
		Error(w, r, fmt.Errorf("acquire halt lock: %w", err), http.StatusInternalServerError)
		return
	}

	/*
		// Checkpoint WAL so we can apply changes directly to the DB.
		if err := db.CheckpointNoLock(r.Context()); err != nil {
			Error(w, r, fmt.Errorf("checkpoint: %w", err), http.StatusInternalServerError)
			return
		}
	*/

	// Return lock ID & position to caller.
	if err := json.NewEncoder(w).Encode(haltLock); err != nil {
		Error(w, r, err, http.StatusInternalServerError)
		return
	}
}

func (s *Server) handleDeleteHalt(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	lockID, err := strconv.ParseInt(q.Get("id"), 10, 64)
	if err != nil {
		Error(w, r, fmt.Errorf("invalid id: %q", q.Get("id")), http.StatusBadRequest)
		return
	}

	// Cannot issue remote halt lock from this node.
	if id := r.Header.Get("Litefs-Id"); id == s.store.ID() {
		Error(w, r, fmt.Errorf("cannot remotely unhalt self"), http.StatusBadRequest)
		return
	}

	// Database should have been created from original halt lock.
	db := s.store.DB(name)
	if err != nil {
		Error(w, r, fmt.Errorf("database not found: %q", name), http.StatusNotFound)
		return
	}

	db.ReleaseHaltLock(r.Context(), lockID)

	/*
		// Checkpoint WAL so we can apply changes directly to the DB.
		if err := db.CheckpointNoLock(r.Context()); err != nil {
			Error(w, r, fmt.Errorf("checkpoint: %w", err), http.StatusInternalServerError)
			return
		}
	*/
}

func (s *Server) handlePostTx(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")

	// Cannot issue remote halt lock from this node.
	if id := r.Header.Get("Litefs-Id"); id == s.store.ID() {
		Error(w, r, fmt.Errorf("cannot remotely halt self"), http.StatusBadRequest)
		return
	}

	// Ensure database should already exist from halt lock.
	db := s.store.DB(name)
	if db == nil {
		Error(w, r, fmt.Errorf("database not found: %q", name), http.StatusNotFound)
		return
	}

	/*
		// Checkpoint WAL so we can apply changes directly to the DB.
		if err := db.CheckpointNoLock(r.Context()); err != nil {
			Error(w, r, fmt.Errorf("checkpoint: %w", err), http.StatusInternalServerError)
			return
		}
	*/

	// TODO(fwd): Ensure halt lock is held by caller.
	// TODO(fwd): Prevent halt lock release during copy & apply.

	// Wrap request body in a chunked reader.
	ltxPath, err := db.WriteLTXFileAt(r.Context(), r.Body)
	if err != nil {
		Error(w, r, fmt.Errorf("write ltx file: %s", err), http.StatusInternalServerError)
		return
	}

	// Apply transaction to database.
	if err := db.ApplyLTXNoLock(r.Context(), ltxPath); err != nil {
		Error(w, r, fmt.Errorf("cannot apply ltx: %s", err), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handlePostStream(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor < 2 {
		http.Error(w, "Upgrade to HTTP/2 required", http.StatusUpgradeRequired)
		return
	}

	// Prevent nodes from connecting to themselves.
	if id := r.Header.Get("Litefs-Id"); id == s.store.ID() {
		Error(w, r, fmt.Errorf("cannot connect to self"), http.StatusBadRequest)
		return
	}

	// Wrap context so that it cancels when the primary lease is lost.
	r = r.WithContext(s.store.PrimaryCtx(r.Context()))
	if err := r.Context().Err(); err != nil {
		Error(w, r, err, http.StatusServiceUnavailable)
		return
	}

	log.Printf("%s: stream connected", s.store.ID())
	defer log.Printf("%s: stream disconnected", s.store.ID())

	serverStreamCountMetric.Inc()
	defer serverStreamCountMetric.Dec()

	// Subscribe to store changes
	subscription := s.store.Subscribe()
	defer func() { _ = subscription.Close() }()

	// Read in pos map.
	posMap, err := ReadPosMapFrom(r.Body)
	if err != nil {
		Error(w, r, err, http.StatusBadRequest)
		return
	}

	dbs := s.store.DBs()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].Name() < dbs[j].Name() })

	// Build initial dirty set of databases.
	dirtySet := make(map[string]struct{})
	for name := range posMap {
		dirtySet[name] = struct{}{}
	}
	for _, db := range dbs {
		dirtySet[db.Name()] = struct{}{}
	}

	// Flush header so client can resume control.
	w.WriteHeader(http.StatusOK)
	w.(http.Flusher).Flush()

	// Attempt to flush an "end" frame on disconnect so we can flush it.
	// See: https://github.com/superfly/litefs/issues/182
	defer func() {
		_ = litefs.WriteStreamFrame(w, &litefs.EndStreamFrame{})
		w.(http.Flusher).Flush()
	}()

	// Continually iterate by writing dirty changes and then waiting for new changes.
	var readySent bool
	for {
		// Send pending transactions for each database.
		for name := range dirtySet {
			if err := s.streamDB(r.Context(), w, name, posMap); err != nil {
				Error(w, r, fmt.Errorf("stream error: db=%q err=%s", name, err), http.StatusInternalServerError)
				return
			}
		}

		// Send "ready" frame after initial replication set
		if !readySent {
			if err := litefs.WriteStreamFrame(w, &litefs.ReadyStreamFrame{}); err != nil {
				Error(w, r, fmt.Errorf("stream error: write ready frame: %s", err), http.StatusInternalServerError)
				return
			}
			w.(http.Flusher).Flush()

			readySent = true
		}

		// Wait for new changes, repeat.
		select {
		case <-s.ctx.Done():
			return // server disconnect
		case <-r.Context().Done():
			return // client disconnect
		case <-subscription.NotifyCh():
			dirtySet = subscription.DirtySet()
		}
	}
}

func (s *Server) streamDB(ctx context.Context, w http.ResponseWriter, name string, posMap map[string]litefs.Pos) error {
	db := s.store.DB(name)

	// If the replica has a database that doesn't exist on the primary, skip it.
	// TODO: Send a deletion message to the replica to remove the database.
	if db == nil {
		log.Printf("database not found, skipping: name=%q", name)
		return nil
	}

	for {
		clientPos := posMap[name]
		dbPos := db.Pos()

		// Invalidate client position if we're beyond the primary's TXID.
		// This can occur if an old primary has unreplicated transactions and
		// then loses its primary status and reconnects. By invalidating, we
		// will cause a snapshot to occur.
		if clientPos.TXID > dbPos.TXID {
			log.Printf("client transaction id (%s) exceeds primary transaction id (%s), clearing client position", ltx.FormatTXID(clientPos.TXID), ltx.FormatTXID(dbPos.TXID))
			clientPos = litefs.Pos{}
		}

		// Invalidate client position if the TXID matches but the checksum does not.
		// This can also occur if an old primary has unreplicated transactions.
		if clientPos.TXID == dbPos.TXID && clientPos.PostApplyChecksum != dbPos.PostApplyChecksum {
			log.Printf("client transaction id (%s) caught up but checksum is mismatched (%016x <> %016x), clearing client position", ltx.FormatTXID(clientPos.TXID), clientPos.PostApplyChecksum, dbPos.PostApplyChecksum)
			clientPos = litefs.Pos{}
		}

		// Exit when client has caught up.
		if clientPos.TXID >= dbPos.TXID {
			return nil
		}

		newPos, err := s.streamLTX(ctx, w, db, clientPos.TXID+1, clientPos.PostApplyChecksum)
		if err != nil {
			return fmt.Errorf("stream ltx (%s): %w", ltx.FormatTXID(clientPos.TXID+1), err)
		}
		posMap[name] = newPos
	}
}

func (s *Server) streamLTX(ctx context.Context, w http.ResponseWriter, db *litefs.DB, txID uint64, preApplyChecksum uint64) (newPos litefs.Pos, err error) {
	// Open LTX file, read header.
	f, err := db.OpenLTXFile(txID)
	if os.IsNotExist(err) {
		log.Printf("transaction file for txid %s no longer available, writing snapshot", ltx.FormatTXID(txID))
		return s.streamLTXSnapshot(ctx, w, db)
	} else if err != nil {
		return litefs.Pos{}, fmt.Errorf("open ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Verify LTX file before sending it to client.
	// OPTIMIZE: This could be skipped in the future. It's mostly here for safety.
	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return litefs.Pos{}, fmt.Errorf("verify ltx: %w", err)
	} else if _, err := f.Seek(0, io.SeekStart); err != nil {
		return litefs.Pos{}, fmt.Errorf("seek ltx to start: %w", err)
	}

	// If previous checksum on client does not match, return snapshot instead.
	if dec.Header().PreApplyChecksum != preApplyChecksum {
		log.Printf("client preapply checksum mismatch for txid %s, writing snapshot", ltx.FormatTXID(txID))
		return s.streamLTXSnapshot(ctx, w, db)
	}

	// Write frame.
	frame := litefs.LTXStreamFrame{Name: db.Name()}
	if err := litefs.WriteStreamFrame(w, &frame); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx stream frame: %w", err)
	}

	// Write LTX file as a chunked byte stream.
	cw := chunk.NewWriter(w)
	if _, err := io.Copy(cw, f); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx chunked stream: %w", err)
	}
	if err := cw.Close(); err != nil {
		return litefs.Pos{}, fmt.Errorf("close ltx chunked stream: %w", err)
	}
	w.(http.Flusher).Flush()

	serverFrameSendCountMetricVec.WithLabelValues(db.Name(), "ltx")

	return litefs.Pos{TXID: dec.Header().MaxTXID, PostApplyChecksum: dec.Trailer().PostApplyChecksum}, nil
}

func (s *Server) streamLTXSnapshot(ctx context.Context, w http.ResponseWriter, db *litefs.DB) (newPos litefs.Pos, err error) {
	// Write frame.
	if err := litefs.WriteStreamFrame(w, &litefs.LTXStreamFrame{Name: db.Name()}); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx snapshot stream frame: %w", err)
	}

	// Write snapshot to writer.
	cw := chunk.NewWriter(w)
	header, trailer, err := db.WriteSnapshotTo(ctx, cw)
	if err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx snapshot to chunked stream: %w", err)
	} else if err := cw.Close(); err != nil {
		return litefs.Pos{}, fmt.Errorf("close ltx snapshot to chunked stream: %w", err)
	}
	w.(http.Flusher).Flush()

	serverFrameSendCountMetricVec.WithLabelValues(db.Name(), "ltx:snapshot")

	return litefs.Pos{TXID: header.MaxTXID, PostApplyChecksum: trailer.PostApplyChecksum}, nil
}

func Error(w http.ResponseWriter, r *http.Request, err error, code int) {
	log.Printf("http: %s %s: error: %s", r.Method, r.URL.Path, err)
	http.Error(w, err.Error(), code)
}

// HTTP server metrics.
var (
	serverStreamCountMetric = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "litefs_http_stream_count",
		Help: "Number of streams currently connected.",
	})

	serverFrameSendCountMetricVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litefs_http_frame_send_count",
		Help: "Number of frames sent.",
	}, []string{"db", "type"})
)
