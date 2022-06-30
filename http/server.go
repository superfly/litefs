package http

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	httppprof "net/http/pprof"
	"sort"
	"strings"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// Server represents an HTTP API server for LiteFS.
type Server struct {
	ln net.Listener

	httpServer  *http.Server
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
	s.httpServer = &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
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
		if e := s.ln.Close(); e != nil && err == nil {
			err = e
		}
	}

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
			httppprof.Cmdline(w, r)
		case "/debug/pprof/profile":
			httppprof.Profile(w, r)
		case "/debug/pprof/symbol":
			httppprof.Symbol(w, r)
		case "/debug/pprof/trace":
			httppprof.Trace(w, r)
		default:
			httppprof.Index(w, r)
		}
		return
	}

	switch r.URL.Path {
	case "/metrics":
		s.promHandler.ServeHTTP(w, r)

	case "/stream":
		switch r.Method {
		case http.MethodPost:
			s.handlePostStream(w, r)
		default:
			Error(w, r, fmt.Errorf("Method not allowed"), http.StatusMethodNotAllowed)
		}
	default:
		http.NotFound(w, r)
	}
}

func (s *Server) handlePostStream(w http.ResponseWriter, r *http.Request) {
	log.Printf("stream connected")
	defer log.Printf("stream disconnected")

	// Subscribe to store changes
	subscription := s.store.Subscribe()
	defer subscription.Close()

	// Read in pos map.
	posMap, err := ReadPosMapFrom(r.Body)
	if err != nil {
		Error(w, r, err, http.StatusBadRequest)
		return
	}

	dbs := s.store.DBs()
	sort.Slice(dbs, func(i, j int) bool { return dbs[i].ID() < dbs[j].ID() })

	// Fill any missing databases with empty positions.
	for _, db := range dbs {
		if _, ok := posMap[db.ID()]; !ok {
			posMap[db.ID()] = litefs.Pos{}
		}
	}

	// Build initial dirty set of databases.
	dirtySet := make(map[uint64]struct{})
	for dbID := range posMap {
		dirtySet[dbID] = struct{}{}
	}

	// Send initial database info.
	for _, db := range dbs {
		frame := litefs.DBStreamFrame{DBID: db.ID(), Name: db.Name()}
		if err := litefs.WriteStreamFrame(w, &frame); err != nil {
			Error(w, r, fmt.Errorf("stream error: write db stream frame: %w", err), http.StatusInternalServerError)
			return
		}
	}

	// Continually iterate by writing dirty changes and then waiting for new changes.
	for {
		// Send pending transactions for each database.
		for dbID := range dirtySet {
			if err := s.streamDB(r.Context(), w, dbID, posMap); err != nil {
				Error(w, r, fmt.Errorf("stream error: db=%s err=%s", litefs.FormatDBID(dbID), err), http.StatusInternalServerError)
				return
			}
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

func (s *Server) streamDB(ctx context.Context, w http.ResponseWriter, dbID uint64, posMap map[uint64]litefs.Pos) error {
	db := s.store.FindDB(dbID)

	for {
		clientPos := posMap[dbID]
		dbPos := db.Pos()

		// Exit when client has caught up.
		if clientPos.TXID >= dbPos.TXID {
			return nil
		}

		newPos, err := s.streamLTX(ctx, w, db, clientPos.TXID+1)
		if err != nil {
			return fmt.Errorf("stream ltx: pos=%d", clientPos.TXID)
		}
		posMap[dbID] = newPos
	}
}

func (s *Server) streamLTX(ctx context.Context, w http.ResponseWriter, db *litefs.DB, txID uint64) (newPos litefs.Pos, err error) {
	// Open LTX file, read header.
	f, err := db.OpenLTXFile(txID)
	if err != nil {
		return litefs.Pos{}, fmt.Errorf("open ltx file: %w", err)
	}
	defer f.Close()

	fi, err := f.Stat()
	if err != nil {
		return litefs.Pos{}, fmt.Errorf("stat ltx file: %w", err)
	}

	// Parse header to extract max TXID & checksum.
	buf := make([]byte, ltx.HeaderSize)
	var hdr ltx.Header
	if _, err := io.ReadFull(f, buf); err != nil {
		return litefs.Pos{}, fmt.Errorf("read ltx header: %w", err)
	} else if err := hdr.UnmarshalBinary(buf); err != nil {
		return litefs.Pos{}, fmt.Errorf("unmarshal ltx header: %w", err)
	}

	// Write frame.
	frame := litefs.LTXStreamFrame{Size: fi.Size()}
	if err := litefs.WriteStreamFrame(w, &frame); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx stream frame: %w", err)
	}

	// Write LTX file.
	if _, err := w.Write(buf); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx header: %w", err)
	} else if _, err := io.CopyN(w, f, frame.Size-int64(len(buf))); err != nil {
		return litefs.Pos{}, fmt.Errorf("write ltx file: %w", err)
	}
	w.(http.Flusher).Flush()

	return litefs.Pos{TXID: hdr.MaxTXID}, nil
}

func Error(w http.ResponseWriter, r *http.Request, err error, code int) {
	log.Printf("http: error: %s", err)
	http.Error(w, err.Error(), code)
}
