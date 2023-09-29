package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal/chunk"
	"github.com/superfly/ltx"
	"golang.org/x/net/http2"
)

var _ litefs.Client = (*Client)(nil)

// Client represents an client for a streaming LiteFS HTTP server.
type Client struct {
	// Underlying HTTP client
	HTTPClient *http.Client
}

// NewClient returns an instance of Client.
func NewClient() *Client {
	return &Client{
		HTTPClient: &http.Client{
			Transport: &http2.Transport{
				AllowHTTP: true,
				DialTLS: func(network, addr string, cfg *tls.Config) (net.Conn, error) {
					return net.Dial(network, addr) // h2c-only right now
				},
			},
		},
	}
}

// Promote attempts to promote the current node to be the primary.
func (c *Client) Promote(ctx context.Context, baseURL string) error {
	u, err := url.Parse(baseURL)
	if err != nil {
		return fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return fmt.Errorf("URL host required")
	}
	*u = url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/promote"}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		return litefs.ErrNotEligible
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
	return nil
}

// Handoff requests that the current primary handoff leadership to a specific node.
func (c *Client) Handoff(ctx context.Context, primaryURL string, nodeID uint64) error {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return fmt.Errorf("URL host required")
	}
	*u = url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/handoff"}
	u.RawQuery = (url.Values{"nodeID": {litefs.FormatNodeID(nodeID)}}).Encode()

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode == http.StatusConflict {
		return litefs.ErrNotEligible
	} else if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
	return nil
}

// Import creates or replaces a SQLite database on the remote LiteFS server.
func (c *Client) Import(ctx context.Context, primaryURL, name string, r io.Reader) error {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return fmt.Errorf("URL host required")
	}

	// Strip off everything but the scheme/host & add name to query params.
	*u = url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   "/import",
		RawQuery: (url.Values{
			"name": {name},
		}).Encode(),
	}

	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
	return nil
}

// Export downloads a SQLite database from the remote LiteFS server.
// Returned reader must be closed by caller.
func (c *Client) Export(ctx context.Context, primaryURL, name string) (io.ReadCloser, error) {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return nil, fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return nil, fmt.Errorf("URL host required")
	}

	// Strip off everything but the scheme/host & add name to query params.
	*u = url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   "/export",
		RawQuery: (url.Values{
			"name": {name},
		}).Encode(),
	}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	switch resp.StatusCode {
	case http.StatusOK:
		return resp.Body, nil
	case http.StatusNotFound:
		_ = resp.Body.Close()
		return nil, litefs.ErrDatabaseNotFound
	default:
		_ = resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
}

// Info returns basic information about the node.
func (c *Client) Info(ctx context.Context, baseURL string) (info litefs.NodeInfo, err error) {
	u, err := url.Parse(baseURL)
	if err != nil {
		return info, fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return info, fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return info, fmt.Errorf("URL host required")
	}
	*u = url.URL{Scheme: u.Scheme, Host: u.Host, Path: "/info"}

	req, err := http.NewRequest("GET", u.String(), nil)
	if err != nil {
		return info, err
	}
	req = req.WithContext(ctx)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return info, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return info, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	if err := json.NewDecoder(resp.Body).Decode(&info); err != nil {
		return info, fmt.Errorf("decode body: %w", err)
	}

	return info, nil
}

func (c *Client) AcquireHaltLock(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) (_ *litefs.HaltLock, retErr error) {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return nil, fmt.Errorf("invalid primary URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return nil, fmt.Errorf("URL host required")
	}

	// Strip off everything but the scheme & host.
	*u = url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   "/halt",
		RawQuery: (url.Values{
			"name": []string{name},
			"id":   []string{strconv.FormatInt(lockID, 10)},
		}).Encode(),
	}

	req, err := http.NewRequest("POST", u.String(), nil)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)
	req.Header.Set(HeaderNodeID, litefs.FormatNodeID(nodeID))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	// If unsuccessful, close stream and return an error.
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	var haltLock litefs.HaltLock
	if err := json.NewDecoder(resp.Body).Decode(&haltLock); err != nil {
		return nil, err
	}
	return &haltLock, nil
}

func (c *Client) ReleaseHaltLock(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) error {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return fmt.Errorf("invalid primary URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return fmt.Errorf("URL host required")
	}

	// Strip off everything but the scheme & host.
	*u = url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   "/halt",
		RawQuery: (url.Values{
			"name": []string{name},
			"id":   []string{strconv.FormatInt(lockID, 10)},
		}).Encode(),
	}

	req, err := http.NewRequest("DELETE", u.String(), nil)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set(HeaderNodeID, litefs.FormatNodeID(nodeID))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// If unsuccessful, close stream and return an error.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
	return nil
}

func (c *Client) Commit(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64, r io.Reader) error {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return fmt.Errorf("invalid primary URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return fmt.Errorf("URL host required")
	}

	// Strip off everything but the scheme & host.
	*u = url.URL{
		Scheme: u.Scheme,
		Host:   u.Host,
		Path:   "/tx",
		RawQuery: (url.Values{
			"name":   []string{name},
			"lockID": []string{strconv.FormatInt(lockID, 10)},
		}).Encode(),
	}

	req, err := http.NewRequest("POST", u.String(), r)
	if err != nil {
		return err
	}
	req = req.WithContext(ctx)
	req.Header.Set(HeaderNodeID, litefs.FormatNodeID(nodeID))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer func() { _ = resp.Body.Close() }()

	// If unsuccessful, close stream and return an error.
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
	return nil
}

// Stream returns a snapshot and continuous stream of WAL updates.
func (c *Client) Stream(ctx context.Context, primaryURL string, nodeID uint64, posMap map[string]ltx.Pos, filter []string) (litefs.Stream, error) {
	u, err := url.Parse(primaryURL)
	if err != nil {
		return nil, fmt.Errorf("invalid client URL: %w", err)
	} else if u.Scheme != "http" && u.Scheme != "https" {
		return nil, fmt.Errorf("invalid URL scheme")
	} else if u.Host == "" {
		return nil, fmt.Errorf("URL host required")
	}

	q := make(url.Values)
	if len(filter) > 0 {
		q.Set("filter", strings.Join(filter, ","))
	}

	// Strip off everything but the scheme & host.
	*u = url.URL{
		Scheme:   u.Scheme,
		Host:     u.Host,
		Path:     "/stream",
		RawQuery: q.Encode(),
	}

	var buf bytes.Buffer
	if err := WritePosMapTo(&buf, posMap); err != nil {
		return nil, fmt.Errorf("cannot write pos map: %w", err)
	}

	req, err := http.NewRequest("POST", u.String(), &buf)
	if err != nil {
		return nil, err
	}
	req = req.WithContext(ctx)

	req.Header.Set(HeaderNodeID, litefs.FormatNodeID(nodeID))

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	return &Stream{
		ReadCloser: resp.Body,
		clusterID:  resp.Header.Get(HeaderClusterID),
	}, nil
}

var _ litefs.Stream = (*Stream)(nil)

// Stream is a wrapper around the stream response body.
type Stream struct {
	io.ReadCloser

	clusterID string
}

// ClusterID returns the cluster ID found in the response header.
func (s *Stream) ClusterID() string { return s.clusterID }

// RemoteTx represents a remote transaction created by Client.Begin().
type RemoteTx struct {
	id               uint64
	preApplyChecksum uint64
	committed        bool

	w io.WriteCloser // request body
	r io.ReadCloser  // response body
}

// ID returns the TXID of the transaction.
func (tx *RemoteTx) ID() uint64 { return tx.id }

// PreApplyChecksum returns checksum of the database before the transaction is applied.
func (tx *RemoteTx) PreApplyChecksum() uint64 { return tx.preApplyChecksum }

// Commit send the LTX data from r to the remote primary for commit.
func (tx *RemoteTx) Commit(r io.Reader) error {
	if tx.committed {
		return nil
	}

	defer func() { _ = tx.Rollback() }()

	cw := chunk.NewWriter(tx.w)
	if _, err := io.Copy(cw, r); err != nil {
		return fmt.Errorf("write ltx chunked stream: %w", err)
	} else if err := cw.Close(); err != nil {
		return fmt.Errorf("close ltx chunked stream: %w", err)
	}

	// Read an integer status code from the binary to confirm commit.
	// This should return a code of zero on success.
	var ret uint32
	if err := binary.Read(tx.r, binary.BigEndian, &ret); err != nil {
		return fmt.Errorf("read remote tx confirmation: %w", err)
	} else if ret != 0 {
		return fmt.Errorf("remote tx confirmation failed (%d)", ret)
	}

	tx.committed = true

	return nil
}

// Rollback closes the connection without sending a transaction, thus rolling back.
func (tx *RemoteTx) Rollback() error {
	if err := tx.w.Close(); err != nil {
		_ = tx.r.Close()
		return fmt.Errorf("close writer: %w", err)
	}
	if err := tx.r.Close(); err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}
