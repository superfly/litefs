package http

import (
	"context"
	"encoding/binary"
	"expvar"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/sync/errgroup"
)

// Default settings
const (
	DefaultAddr = ":20202"
)

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
	cancel func()
}

func NewServer(store *litefs.Store, addr string) *Server {
	s := &Server{
		addr:  addr,
		store: store,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

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
	s.cancel()
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
	if strings.HasPrefix(r.URL.Path, "/debug") {
		switch r.URL.Path {
		case "/debug/vars":
			expvar.Handler().ServeHTTP(w, r)
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
	if r.URL.Path == "/metrics" {
		s.promHandler.ServeHTTP(w, r)
		return
	}

	// Require HTTP/2 for all internal endpoints.
	if r.ProtoMajor < 2 {
		http.Error(w, "Upgrade to HTTP/2 required", http.StatusUpgradeRequired)
		return
	}

	switch r.URL.Path {
	case "/tx":
		switch r.Method {
		case http.MethodPost:
			s.handlePostTx(w, r)
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

func (s *Server) handlePostTx(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")

	db, err := s.store.CreateDBIfNotExists(name)
	if err != nil {
		Error(w, r, fmt.Errorf("create db: %w", err), http.StatusInternalServerError)
		return
	}

	// Acquire file locks on the database for the transction.
	guardSet, err := db.AcquireWriteLock(r.Context())
	if err != nil {
		Error(w, r, fmt.Errorf("acquire write lock: %w", err), http.StatusInternalServerError)
		return
	}
	defer guardSet.Unlock() // release on connection close

	// Write transaction metadata back to client.
	pos := db.Pos()
	txID, preApplyChecksum := uint64(pos.TXID+1), pos.PostApplyChecksum
	if err := binary.Write(w, binary.BigEndian, txID); err != nil {
		Error(w, r, fmt.Errorf("write txid: %w", err), http.StatusInternalServerError)
		return
	}
	if err := binary.Write(w, binary.BigEndian, preApplyChecksum); err != nil {
		Error(w, r, fmt.Errorf("write pre-apply checksum: %w", err), http.StatusInternalServerError)
		return
	}

	w.(http.Flusher).Flush()

	// Apply transaction to database.
	if err := db.ApplyLTX(r.Context(), r.Body, false); err != nil {
		Error(w, r, err, http.StatusInternalServerError)
		return
	}

	// TODO(fwd): Return confirmation in trailer, maybe?

}

func (s *Server) handlePostStream(w http.ResponseWriter, r *http.Request) {
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

	log.Printf("stream connected")
	defer log.Printf("stream disconnected")

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
		case <-r.Context().Done():
			return
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

		// Exit when client has caught up.
		if clientPos.TXID >= dbPos.TXID {
			return nil
		}

		newPos, err := s.streamLTX(ctx, w, db, clientPos.TXID+1)
		if err != nil {
			return fmt.Errorf("stream ltx (tx %d): %w", clientPos.TXID, err)
		}
		posMap[name] = newPos
	}
}

func (s *Server) streamLTX(ctx context.Context, w http.ResponseWriter, db *litefs.DB, txID uint64) (newPos litefs.Pos, err error) {
	// Open LTX file, read header.
	f, err := db.OpenLTXFile(txID)
	if os.IsNotExist(err) {
		return s.streamLTXSnapshot(ctx, w, db)
	} else if err != nil {
		return litefs.Pos{}, fmt.Errorf("open ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()

	r := ltx.NewReader(f)
	if err := r.PeekHeader(); err != nil {
		return litefs.Pos{}, fmt.Errorf("peek ltx header: %w", err)
	}

	// Write frame.
	frame := litefs.LTXStreamFrame{Name: db.Name()}
	if err := litefs.WriteStreamFrame(w, &frame); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx stream frame: %w", err)
	}

	// Write LTX file.
	n, err := io.Copy(w, r)
	if err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx file: %w", err)
	}
	w.(http.Flusher).Flush()

	log.Printf("send frame<ltx>: db=%q tx=%s-%s size=%d", db.Name(), ltx.FormatTXID(r.Header().MinTXID), ltx.FormatTXID(r.Header().MaxTXID), n)

	serverFrameSendCountMetricVec.WithLabelValues(db.Name(), "ltx")

	return litefs.Pos{TXID: r.Header().MaxTXID, PostApplyChecksum: r.Trailer().PostApplyChecksum}, nil
}

func (s *Server) streamLTXSnapshot(ctx context.Context, w http.ResponseWriter, db *litefs.DB) (newPos litefs.Pos, err error) {
	// Write frame.
	if err := litefs.WriteStreamFrame(w, &litefs.LTXStreamFrame{Name: db.Name()}); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx snapshot stream frame: %w", err)
	}

	// Write snapshot to writer.
	header, trailer, err := db.WriteSnapshotTo(ctx, w)
	if err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx snapshot file: %w", err)
	}
	w.(http.Flusher).Flush()

	log.Printf("send frame<ltx>: db=%q tx=(%s,%s) chksum=(%x,%x) (snapshot)",
		db.Name(), ltx.FormatTXID(header.MinTXID), ltx.FormatTXID(header.MaxTXID),
		header.PreApplyChecksum, trailer.PostApplyChecksum)

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
