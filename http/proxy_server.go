package http

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"regexp"
	"syscall"
	"time"

	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// TXIDCookieName is the name of the cookie that tracks transaction ID.
const TXIDCookieName = "__txid"

const (
	DefaultPollTXIDInterval = 1 * time.Millisecond
	DefaultPollTXIDTimeout  = 5 * time.Second

	DefaultCookieExpiry = 5 * time.Minute
)

var ErrProxyServerClosed = fmt.Errorf("canceled, proxy server closed")

// ProxyServer represents a thin proxy in front of the user's application that can
// handle primary redirection and TXID consistency on replicas.
//
// Exported fields must be set before calling Listen().
type ProxyServer struct {
	ln         net.Listener
	httpServer *http.Server
	store      *litefs.Store

	g      errgroup.Group
	ctx    context.Context
	cancel context.CancelCauseFunc

	// Hostport of application that is being proxied.
	Target string

	// Name of database to use for TXID consistency tracking.
	DBName string

	// Bind address that the proxy listens on.
	Addr string

	// List of path expressions that will be passed through if matched.
	Passthroughs []*regexp.Regexp

	// If true, add verbose debug logging.
	Debug bool

	// Interval & timeout for ensuring read consistency.
	PollTXIDInterval time.Duration
	PollTXIDTimeout  time.Duration

	// Time before cookie expires on client.
	CookieExpiry time.Duration

	HTTPTransport *http.Transport
}

// NewProxyServer returns a new instance of ProxyServer.
func NewProxyServer(store *litefs.Store) *ProxyServer {
	s := &ProxyServer{
		store: store,

		PollTXIDInterval: DefaultPollTXIDInterval,
		PollTXIDTimeout:  DefaultPollTXIDTimeout,
		CookieExpiry:     DefaultCookieExpiry,
	}

	s.ctx, s.cancel = context.WithCancelCause(context.Background())

	s.httpServer = &http.Server{
		Handler: http.HandlerFunc(s.serveHTTP),
	}

	s.HTTPTransport = &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: dialContextWithRetry(&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}),
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return s
}

func (s *ProxyServer) Listen() (err error) {
	if s.Target == "" {
		return fmt.Errorf("proxy target required")
	}
	if s.DBName == "" {
		return fmt.Errorf("proxy database name required")
	}
	if s.Addr == "" {
		return fmt.Errorf("proxy bind address required")
	}

	s.ln, err = net.Listen("tcp", s.Addr)
	return err
}

func (s *ProxyServer) Serve() {
	s.g.Go(func() error {
		if err := s.httpServer.Serve(s.ln); s.ctx.Err() != nil {
			return err
		}
		return nil
	})
}

func (s *ProxyServer) Close() (err error) {
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

	s.cancel(ErrProxyServerClosed)
	if e := s.g.Wait(); e != nil && err == nil {
		err = e
	}
	return err
}

// Port returns the port the listener is running on.
func (s *ProxyServer) Port() int {
	if s.ln == nil {
		return 0
	}
	return s.ln.Addr().(*net.TCPAddr).Port
}

// URL returns the full base URL for the running server.
func (s *ProxyServer) URL() string {
	host, _, _ := net.SplitHostPort(s.Addr)
	if host == "" {
		host = "localhost"
	}
	return fmt.Sprintf("http://%s", net.JoinHostPort(host, fmt.Sprint(s.Port())))
}

func (s *ProxyServer) serveHTTP(w http.ResponseWriter, r *http.Request) {
	// If request matches any passthrough regexes, send directly to target.
	if s.isPassthrough(r) {
		s.logf("proxy: %s %s: matches passthrough expression, proxying to target", r.Method, r.URL.Path)
		s.proxyToTarget(w, r, true)
		return
	}

	switch r.Method {
	case http.MethodGet:
		s.serveRead(w, r)
	case http.MethodHead:
		s.serveRead(w, r)
	default:
		s.serveNonRead(w, r)
	}
}

func (s *ProxyServer) serveRead(w http.ResponseWriter, r *http.Request) {
	// Determine the last write TXID seen by
	var txid ltx.TXID
	if cookie, _ := r.Cookie(TXIDCookieName); cookie != nil {
		txid, _ = ltx.ParseTXID(cookie.Value)
	}

	// No TXID or we couldn't parse it. Just send to the target.
	if txid == 0 {
		s.logf("proxy: %s %s: no client txid, proxying to target", r.Method, r.URL.Path)
		s.proxyToTarget(w, r, false)
		return
	}

	// Lookup our database that we use for TXID tracking.
	// If the database hasn't been created yet, just send to target.
	db := s.store.DB(s.DBName)
	if db == nil {
		s.logf("proxy: %s %s: no database %q, proxying to target", r.Method, r.URL.Path, s.DBName)
		s.proxyToTarget(w, r, false)
		return
	}

	// Wait for database to catch up to TXID.
	ticker := time.NewTicker(s.PollTXIDInterval)
	defer ticker.Stop()

	ctx, cancel := context.WithTimeout(r.Context(), s.PollTXIDTimeout)
	defer cancel()

	var pos ltx.Pos
LOOP:
	for {
		if pos = db.Pos(); pos.TXID >= txid {
			s.logf("proxy: %s %s: database %q at txid %s, proxying to target", r.Method, r.URL.Path, s.DBName, pos.TXID.String())
			break LOOP
		}

		select {
		case <-ctx.Done():
			s.logf("proxy: %s %s: database %q at txid %s, requires txid %s, proxy timeout", r.Method, r.URL.Path, s.DBName, pos.TXID.String(), txid.String())
			http.Error(w, "Proxy timeout", http.StatusGatewayTimeout)
			return
		case <-ticker.C:
		}
	}

	// Send request to the target once we've caught up to the last write seen.
	s.proxyToTarget(w, r, false)
}

func (s *ProxyServer) serveNonRead(w http.ResponseWriter, r *http.Request) {
	isPrimary, info := s.store.PrimaryInfo()

	// If this is the primary, send the request to the target.
	if isPrimary {
		s.logf("proxy: %s %s: node is primary, proxying to target", r.Method, r.URL.Path)
		s.proxyToTarget(w, r, false)
		return
	}

	// Look up the hostname of the primary. If there's no primary info then
	// go ahead and send the request
	if info == nil {
		s.logf("proxy: %s %s: no primary available, returning 503", r.Method, r.URL.Path)
		http.Error(w, "Proxy error: no primary available", http.StatusServiceUnavailable)
		return
	}

	// If this is a replica, then we'll redirect the request to the primary.
	w.Header().Set("fly-replay", "instance="+info.Hostname)
}

func (s *ProxyServer) proxyToTarget(w http.ResponseWriter, r *http.Request, passthrough bool) {
	// Update request URL to target server.
	r.URL.Scheme = "http"
	r.URL.Host = s.Target

	resp, err := s.HTTPTransport.RoundTrip(r)
	if err != nil {
		http.Error(w, "Proxy error: "+err.Error(), http.StatusBadGateway)
		return
	}
	defer func() { _ = resp.Body.Close() }()

	// Inject cookie if this is a write and we're not ignoring TXID tracking.
	if !passthrough && s.isWriteRequest(r) {
		if db := s.store.DB(s.DBName); db != nil {
			pos := db.Pos()
			s.logf("proxy: %s %s: setting txid cookie to %s", r.Method, r.URL.Path, pos.TXID.String())
			http.SetCookie(w, &http.Cookie{
				Name:     TXIDCookieName,
				Value:    pos.TXID.String(),
				Path:     "/",
				Expires:  time.Now().Add(s.CookieExpiry),
				HttpOnly: true,
			})
		}
	}

	// Copy response headers
	for key, values := range resp.Header {
		for _, v := range values {
			w.Header().Add(key, v)
		}
	}

	// Set response code and copy the body.
	w.WriteHeader(resp.StatusCode)
	if _, err := io.Copy(w, resp.Body); err != nil {
		log.Printf("http: proxy response error: %s", err)
		return
	}
}

func (s *ProxyServer) isWriteRequest(r *http.Request) bool {
	return r.Method != http.MethodGet && r.Method != http.MethodHead
}

// isPassthrough returns true if request matches any of the passthrough expressions.
func (s *ProxyServer) isPassthrough(r *http.Request) bool {
	for _, re := range s.Passthroughs {
		if re.MatchString(r.URL.Path) {
			return true
		}
	}
	return false
}

// logf logs if debug logging is enabled.
func (s *ProxyServer) logf(format string, v ...any) {
	if s.Debug {
		log.Printf(format, v...)
	}
}

// dialContextWithRetry returns a function that will retry
func dialContextWithRetry(dialer *net.Dialer) func(context.Context, string, string) (net.Conn, error) {
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		timeout := time.NewTimer(dialer.Timeout)
		defer timeout.Stop()

		for {
			conn, err := dialer.DialContext(ctx, network, address)
			if !errors.Is(err, syscall.ECONNREFUSED) {
				return conn, err
			}

			select {
			case <-ctx.Done():
				return nil, context.Cause(ctx)
			case <-timeout.C:
				return nil, err
			case <-time.After(100 * time.Millisecond):
			}
		}
	}
}
