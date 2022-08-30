package http

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/superfly/litefs"
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
		HTTPClient: http.DefaultClient,
	}
}

// Stream returns a snapshot and continuous stream of WAL updates.
func (c *Client) Stream(ctx context.Context, rawurl string, nodeID string, posMap map[uint32]litefs.Pos) (io.ReadCloser, error) {
	u, err := url.Parse(rawurl)
	if err != nil {
		return nil, fmt.Errorf("invalid client URL: %w", err)
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
