package http

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/binary"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"

	"github.com/superfly/litefs"
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

func (c *Client) Begin(ctx context.Context, primaryURL string, nodeID, name string) (_ litefs.Tx, retErr error) {
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
		Path:   "/tx",
		RawQuery: (url.Values{
			"name": []string{name},
		}).Encode(),
	}

	pr, pw := io.Pipe()
	req, err := http.NewRequest("POST", u.String(), io.NopCloser(pr))
	if err != nil {
		return nil, err
	}
	// req = req.WithContext(ctx)
	req.Header.Set("Litefs-Id", nodeID)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}

	// If unsuccessful, close stream and return an error.
	if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	// Otherwise capture transaction info and return it to the caller to complete the transaction.
	tx := &Tx{w: pw, r: resp.Body}
	defer func() {
		if retErr != nil {
			_ = tx.Rollback()
		}
	}()

	if err := binary.Read(resp.Body, binary.BigEndian, &tx.id); err != nil {
		return nil, fmt.Errorf("read txid: %w", err)
	}
	if err := binary.Read(resp.Body, binary.BigEndian, &tx.preApplyChecksum); err != nil {
		return nil, fmt.Errorf("read pre-apply checksum: %w", err)
	}

	return tx, nil
}

// Stream returns a snapshot and continuous stream of WAL updates.
func (c *Client) Stream(ctx context.Context, primaryURL string, nodeID string, posMap map[string]litefs.Pos) (io.ReadCloser, error) {
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
		Path:   "/stream",
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

	req.Header.Set("Litefs-Id", nodeID)

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}
	return resp.Body, nil
}

// Tx represents a remote transaction created by Client.Begin().
type Tx struct {
	id               uint64
	preApplyChecksum uint64

	w io.WriteCloser // request body
	r io.ReadCloser  // response body
}

// ID returns the TXID of the transaction.
func (tx *Tx) ID() uint64 { return tx.id }

// PreApplyChecksum returns checksum of the database before the transaction is applied.
func (tx *Tx) PreApplyChecksum() uint64 { return tx.preApplyChecksum }

// Commit send the LTX data from r to the remote primary for commit.
func (tx *Tx) Commit(r io.Reader) error {
	defer func() { _ = tx.Rollback() }()

	if _, err := io.Copy(tx.w, r); err != nil {
		return fmt.Errorf("copy ltx file: %w", err)
	}

	// TODO: Read confirmation from primary.

	return nil
}

// Rollback closes the connection without sending a transaction, thus rolling back.
func (tx *Tx) Rollback() error {
	if err := tx.w.Close(); err != nil {
		_ = tx.r.Close()
		return fmt.Errorf("close writer: %w", err)
	}
	if err := tx.r.Close(); err != nil {
		return fmt.Errorf("close reader: %w", err)
	}
	return nil
}
