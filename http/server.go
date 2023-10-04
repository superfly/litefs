package http

import (
	"context"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/http/pprof"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal"
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

// HTTP headers
const (
	HeaderNodeID    = "Litefs-Id"
	HeaderClusterID = "Litefs-Cluster-Id"
)

const (
	HeartbeatInterval = time.Second
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

	// Time allowed to write a single LTX snapshot in the stream.
	// This is meant to prevent slow snapshot downloads backing up the primary.
	SnapshotTimeout time.Duration
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
	if e := internal.Close(s.ln); e != nil && err == nil {
		err = fmt.Errorf("close listener: %w", e)
	}
	if e := internal.Close(s.httpServer); e != nil && err == nil {
		err = fmt.Errorf("close http server: %w", e)
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
	case "/debug/rand":
		s.handleDebugRand(w, r)
		return
	case "/metrics":
		s.promHandler.ServeHTTP(w, r)
		return
	}

	// Inject standard headers.
	w.Header().Set(HeaderNodeID, litefs.FormatNodeID(s.store.ID()))
	if v := s.store.ClusterID(); v != "" {
		w.Header().Set(HeaderClusterID, v)
	}

	switch r.URL.Path {
	case "/export":
		switch r.Method {
		case http.MethodGet:
			s.handleGetExport(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	case "/halt":
		switch r.Method {
		case http.MethodPost:
			s.handlePostHalt(w, r)
		case http.MethodDelete:
			s.handleDeleteHalt(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	case "/handoff":
		switch r.Method {
		case http.MethodPost:
			s.handlePostHandoff(w, r)
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

	case "/info":
		switch r.Method {
		case http.MethodGet:
			s.handleGetInfo(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	case "/promote":
		switch r.Method {
		case http.MethodPost:
			s.handlePostPromote(w, r)
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

	case "/tx":
		switch r.Method {
		case http.MethodPost:
			s.handlePostTx(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	case "/events":
		switch r.Method {
		case http.MethodGet:
			s.handleGetEvents(w, r)
		default:
			Error(w, r, fmt.Errorf("method not allowed"), http.StatusMethodNotAllowed)
		}

	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handleDebugRand(w http.ResponseWriter, r *http.Request) {
	log.Printf("/debug/rand: connected")
	defer log.Printf("/debug/rand: disconnected")

	ctx, cancel := context.WithTimeout(r.Context(), 1*time.Minute)
	defer cancel()

	rnd := rand.New(rand.NewSource(0))
	buf := make([]byte, 4096)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			_, _ = rnd.Read(buf)
			if _, err := w.Write(buf); err != nil {
				return
			}
		}
	}
}

func (s *Server) handleGetInfo(w http.ResponseWriter, r *http.Request) {
	var info litefs.NodeInfo
	info.ClusterID = s.store.ClusterID()
	info.IsPrimary = s.store.IsPrimary()
	info.Candidate = s.store.Candidate()
	info.Path = s.store.Path()

	if isPrimary, primaryInfo := s.store.PrimaryInfo(); isPrimary {
		info.Primary.Hostname = s.store.Leaser.Hostname()
	} else if primaryInfo != nil {
		info.Primary.Hostname = primaryInfo.Hostname
	}

	if buf, err := json.MarshalIndent(info, "", "  "); err != nil {
		Error(w, r, err, http.StatusInternalServerError)
		return
	} else if _, err := w.Write(buf); err != nil {
		Error(w, r, err, http.StatusInternalServerError)
		return
	}
	_, _ = w.Write([]byte("\n"))
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

func (s *Server) handleGetExport(w http.ResponseWriter, r *http.Request) {
	name := r.URL.Query().Get("name")
	if name == "" {
		Error(w, r, fmt.Errorf("name required"), http.StatusBadRequest)
		return
	}

	// Wrap context so that it cancels when the primary lease is lost.
	if err := r.Context().Err(); err != nil {
		Error(w, r, err, http.StatusServiceUnavailable)
		return
	}

	db := s.store.DB(name)
	if db == nil {
		Error(w, r, litefs.ErrDatabaseNotFound, http.StatusNotFound)
		return
	}

	pos, err := db.Export(r.Context(), w)
	if err != nil {
		Error(w, r, fmt.Errorf("write snapshot: %w", err), http.StatusInternalServerError)
		return
	}

	log.Printf("%s: snapshot successfully exported @ %s", litefs.FormatNodeID(s.store.ID()), pos.String())
}

func (s *Server) handlePostHalt(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")
	lockID, err := strconv.ParseInt(q.Get("id"), 10, 64)
	if err != nil {
		Error(w, r, fmt.Errorf("invalid id: %q", q.Get("id")), http.StatusBadRequest)
		return
	}

	// Cannot issue remote halt lock from this node.
	if id, _ := litefs.ParseNodeID(r.Header.Get(HeaderNodeID)); id == s.store.ID() {
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
	haltLock, err := db.AcquireHaltLock(r.Context(), lockID)
	if err != nil {
		Error(w, r, fmt.Errorf("acquire halt lock: %w", err), http.StatusInternalServerError)
		return
	}

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
	if id, _ := litefs.ParseNodeID(r.Header.Get(HeaderNodeID)); id == s.store.ID() {
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
}

func (s *Server) handlePostPromote(w http.ResponseWriter, r *http.Request) {
	// Return an error if current node is not eligible to become primary.
	if !s.store.Candidate() {
		Error(w, r, litefs.ErrNotEligible, http.StatusConflict)
		return
	}

	// Skip if the node is already the primary.
	isPrimary, info := s.store.PrimaryInfo()
	if isPrimary {
		log.Printf("node is already primary, skipping promotion")
		w.WriteHeader(http.StatusOK)
		return
	}

	// If we are not connected to a primary then an election may be in process.
	//
	// NOTE: This could be cleaned up to wait for the election to finish and
	// not return an error. However, for now we'll simply return an error and
	// the client can try again.
	if info == nil {
		Error(w, r, fmt.Errorf("no primary is currently available for handoff, cannot promote"), http.StatusInternalServerError)
		return
	}

	// Request that the current primary hands off to this node.
	log.Printf("requesting primary handoff from: %s", info.AdvertiseURL)
	client := NewClient()
	if err := client.Handoff(r.Context(), info.AdvertiseURL, s.store.ID()); err != nil {
		Error(w, r, fmt.Errorf("handoff failed: %w", err), http.StatusInternalServerError)
		return
	}
	log.Printf("primary handoff request successful")
}

func (s *Server) handlePostHandoff(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()

	// Parse the requested node ID from the query parameters.
	nodeID, err := litefs.ParseNodeID(q.Get("nodeID"))
	if err != nil {
		Error(w, r, fmt.Errorf("invalid node id"), http.StatusBadRequest)
		return
	}

	// Request handoff from the store. This can fail if the node is not connected.
	if err := s.store.Handoff(r.Context(), nodeID); err != nil {
		Error(w, r, fmt.Errorf("cannot handoff: %w", err), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handlePostTx(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("name")

	// Cannot issue remote halt lock from this node.
	if id, _ := litefs.ParseNodeID(r.Header.Get(HeaderNodeID)); id == s.store.ID() {
		Error(w, r, fmt.Errorf("cannot remotely halt self"), http.StatusBadRequest)
		return
	}

	// Ensure database should already exist from halt lock.
	db := s.store.DB(name)
	if db == nil {
		Error(w, r, fmt.Errorf("database not found: %q", name), http.StatusNotFound)
		return
	}

	// TODO(fwd): Ensure halt lock is held by caller.
	// TODO(fwd): Prevent halt lock release during copy & apply.

	// Wrap request body in a chunked reader.
	ltxPath, err := db.WriteLTXFileAt(r.Context(), r.Body)
	if err != nil {
		Error(w, r, fmt.Errorf("write ltx file: %s", err), http.StatusInternalServerError)
		return
	}

	// Apply transaction to database.
	if err := db.ApplyLTXNoLock(ltxPath, true); err != nil {
		Error(w, r, fmt.Errorf("cannot apply ltx: %s", err), http.StatusInternalServerError)
		return
	}
}

func (s *Server) handlePostStream(w http.ResponseWriter, r *http.Request) {
	if r.ProtoMajor < 2 {
		http.Error(w, "Upgrade to HTTP/2 required", http.StatusUpgradeRequired)
		return
	}
	q := r.URL.Query()

	// Prevent nodes from connecting to themselves.
	id, _ := litefs.ParseNodeID(r.Header.Get(HeaderNodeID))
	if id == s.store.ID() {
		Error(w, r, fmt.Errorf("cannot connect to self"), http.StatusBadRequest)
		return
	}

	// Wrap context so that it cancels when the primary lease is lost.
	r = r.WithContext(s.store.PrimaryCtx(r.Context()))
	if err := r.Context().Err(); err != nil {
		Error(w, r, err, http.StatusServiceUnavailable)
		return
	}

	log.Printf("%s: stream connected (%s)", litefs.FormatNodeID(s.store.ID()), r.RemoteAddr)
	defer log.Printf("%s: stream disconnected (%s)", litefs.FormatNodeID(s.store.ID()), r.RemoteAddr)

	serverStreamCountMetric.Inc()
	defer serverStreamCountMetric.Dec()

	// Subscribe to store changes
	subscription := s.store.SubscribeChangeSet(id)
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

	// Determine filtered set of databases, if any.
	filterSet := make(map[string]struct{})
	if filter := q.Get("filter"); filter != "" {
		for _, name := range strings.Split(filter, ",") {
			filterSet[name] = struct{}{}
		}
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

	tkr := time.NewTicker(HeartbeatInterval)
	defer tkr.Stop()

	// Continually iterate by writing dirty changes and then waiting for new changes.
	var readySent bool
	var handoffLeaseID string
	for {
		// Restrict dirty set to only databases in the filter set.
		if len(filterSet) > 0 && len(dirtySet) > 0 {
			for name := range dirtySet {
				if _, ok := filterSet[name]; !ok {
					delete(dirtySet, name)
				}
			}
		}

		// Send pending transactions for each database.
		for name := range dirtySet {
			if err := s.streamDB(r.Context(), w, name, posMap); err != nil {
				Error(w, r, fmt.Errorf("stream error: db=%q err=%s", name, err), http.StatusInternalServerError)
				return
			}
		}

		sendHeartbeat := !readySent || len(dirtySet) == 0

		// Send "ready" frame after initial replication set
		if !readySent {
			if err := litefs.WriteStreamFrame(w, &litefs.ReadyStreamFrame{}); err != nil {
				Error(w, r, fmt.Errorf("stream error: write ready frame: %s", err), http.StatusInternalServerError)
				return
			}
			w.(http.Flusher).Flush()

			readySent = true
		}

		if sendHeartbeat {
			f := &litefs.HeartbeatStreamFrame{Timestamp: time.Now().UnixMilli()}

			if err := litefs.WriteStreamFrame(w, f); err != nil {
				Error(w, r, fmt.Errorf("stream error: write heartbeat frame: %s", err), http.StatusInternalServerError)
				return
			}
			w.(http.Flusher).Flush()
		}

		// If we have received a handoff request then forward the lease ID and disconnect.
		if handoffLeaseID != "" {
			if err := litefs.WriteStreamFrame(w, &litefs.HandoffStreamFrame{LeaseID: handoffLeaseID}); err != nil {
				Error(w, r, fmt.Errorf("stream error: write handoff frame: %s", err), http.StatusInternalServerError)
				return
			}
			w.(http.Flusher).Flush()
			return
		}

		// Wait for new changes, repeat.
		select {
		case <-s.ctx.Done():
			return // server disconnect
		case <-r.Context().Done():
			return // client disconnect
		case <-subscription.NotifyCh():
			dirtySet = subscription.DirtySet()
		case handoffLeaseID = <-subscription.HandoffCh():
			dirtySet = subscription.DirtySet()
		case <-tkr.C:
			dirtySet = nil
		}

		tkr.Reset(HeartbeatInterval)
	}
}

func (s *Server) streamDB(ctx context.Context, w http.ResponseWriter, name string, posMap map[string]ltx.Pos) error {
	db := s.store.DB(name)

	// If the replica has a database that doesn't exist on the primary, skip it.
	if db == nil {
		if err := litefs.WriteStreamFrame(w, &litefs.DropDBStreamFrame{Name: name}); err != nil {
			return fmt.Errorf("write drop db frame: %w", err)
		}
		w.(http.Flusher).Flush()

		delete(posMap, name)
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
			log.Printf("client transaction id (%s) exceeds primary transaction id (%s), clearing client position", clientPos.TXID.String(), dbPos.TXID.String())
			clientPos = ltx.Pos{}
		}

		// Invalidate client position if the TXID matches but the checksum does not.
		// This can also occur if an old primary has unreplicated transactions.
		if clientPos.TXID == dbPos.TXID && clientPos.PostApplyChecksum != dbPos.PostApplyChecksum {
			log.Printf("client transaction id (%s) caught up but checksum is mismatched (%s <> %s), clearing client position", clientPos.TXID.String(), clientPos.PostApplyChecksum, dbPos.PostApplyChecksum)
			clientPos = ltx.Pos{}
		}

		// Exit when client has caught up.
		if clientPos.TXID >= dbPos.TXID {
			return nil
		}

		newPos, err := s.streamLTX(ctx, w, db, clientPos.TXID+1, clientPos.PostApplyChecksum)
		if err != nil {
			return fmt.Errorf("stream ltx (%s): %w", ltx.TXID(clientPos.TXID+1).String(), err)
		}
		posMap[name] = newPos
	}
}

func (s *Server) streamLTX(ctx context.Context, w http.ResponseWriter, db *litefs.DB, txID ltx.TXID, preApplyChecksum ltx.Checksum) (newPos ltx.Pos, err error) {
	// Always stream snapshot if we are starting from the first transaction.
	// There's an edge case where LTX files originated on the client and that
	// client will skip them if they're seen again (because of write forwarding).
	if txID == 1 {
		log.Printf("starting from txid %s, writing snapshot", txID.String())
		return s.streamLTXSnapshot(ctx, w, db)
	}

	// Open LTX file, read header.
	f, err := db.OpenLTXFile(txID)
	if os.IsNotExist(err) {
		log.Printf("transaction file for txid %s no longer available, writing snapshot", txID.String())
		return s.streamLTXSnapshot(ctx, w, db)
	} else if err != nil {
		return ltx.Pos{}, fmt.Errorf("open ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Verify LTX file before sending it to client.
	// OPTIMIZE: This could be skipped in the future. It's mostly here for safety.
	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return ltx.Pos{}, fmt.Errorf("verify ltx: %w", err)
	} else if _, err := f.Seek(0, io.SeekStart); err != nil {
		return ltx.Pos{}, fmt.Errorf("seek ltx to start: %w", err)
	}

	// If previous checksum on client does not match, return snapshot instead.
	if dec.Header().PreApplyChecksum != preApplyChecksum {
		log.Printf("client preapply checksum mismatch for txid %s, writing snapshot", txID.String())
		return s.streamLTXSnapshot(ctx, w, db)
	}

	// Write frame.
	frame := litefs.LTXStreamFrame{Name: db.Name()}
	if err := litefs.WriteStreamFrame(w, &frame); err != nil {
		return ltx.Pos{}, fmt.Errorf("write ltx stream frame: %w", err)
	}

	// Write LTX file as a chunked byte stream.
	cw := chunk.NewWriter(w)
	if _, err := io.Copy(cw, f); err != nil {
		return ltx.Pos{}, fmt.Errorf("write ltx chunked stream: %w", err)
	}
	if err := cw.Close(); err != nil {
		return ltx.Pos{}, fmt.Errorf("close ltx chunked stream: %w", err)
	}

	serverFrameSendCountMetricVec.WithLabelValues(db.Name(), "ltx")

	// Send current HWM as a separate frame.
	// OPTIMIZE: Only send this when it's been updated or periodically.
	if err := litefs.WriteStreamFrame(w, &litefs.HWMStreamFrame{Name: db.Name(), TXID: db.HWM()}); err != nil {
		return ltx.Pos{}, fmt.Errorf("write hwm stream frame: %w", err)
	}

	w.(http.Flusher).Flush()

	return ltx.Pos{TXID: dec.Header().MaxTXID, PostApplyChecksum: dec.Trailer().PostApplyChecksum}, nil
}

func (s *Server) streamLTXSnapshot(ctx context.Context, w http.ResponseWriter, db *litefs.DB) (newPos ltx.Pos, err error) {
	// Default the timeout to the retention period if not explicitly set.
	// If a LTX file takes longer than this to download then the next LTX file
	// will be gone before the download is complete.
	timeout := s.SnapshotTimeout
	if timeout == 0 {
		timeout = s.store.Retention
	}
	ctx, cancel := context.WithTimeoutCause(ctx, timeout, fmt.Errorf("snapshot timeout exceeded (%s)", timeout))
	defer cancel()

	// Write frame.
	if err := litefs.WriteStreamFrame(w, &litefs.LTXStreamFrame{Name: db.Name()}); err != nil {
		return ltx.Pos{}, fmt.Errorf("write ltx snapshot stream frame: %w", err)
	}

	// Write snapshot to writer.
	cw := chunk.NewWriter(w)
	header, trailer, err := db.WriteSnapshotTo(ctx, cw)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("write ltx snapshot to chunked stream: %w", err)
	} else if err := cw.Close(); err != nil {
		return ltx.Pos{}, fmt.Errorf("close ltx snapshot to chunked stream: %w", err)
	}
	w.(http.Flusher).Flush()

	serverFrameSendCountMetricVec.WithLabelValues(db.Name(), "ltx:snapshot")

	return ltx.Pos{TXID: header.MaxTXID, PostApplyChecksum: trailer.PostApplyChecksum}, nil
}

func (s *Server) handleGetEvents(w http.ResponseWriter, r *http.Request) {
	subscription := s.store.SubscribeEvents()
	defer func() { subscription.Stop() }()

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	enc := json.NewEncoder(w)
	for {
		select {
		case <-r.Context().Done():
			return
		case event, ok := <-subscription.C():
			if !ok {
				log.Printf("http: event stream buffer exceeded, disconnecting")
				return
			}
			if err := enc.Encode(event); err != nil {
				log.Printf("http: event stream error: %s", err)
				return
			}
			w.(http.Flusher).Flush()
		}
	}
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
