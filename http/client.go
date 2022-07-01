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
func (c *Client) Stream(ctx context.Context, rawurl string, posMap map[uint32]litefs.Pos) (litefs.StreamReader, error) {
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

	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	} else if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("invalid response: code=%d", resp.StatusCode)
	}

	return &StreamReader{
		rc: resp.Body,
		lr: io.LimitedReader{R: resp.Body},
	}, nil
}

// StreamReader represents a stream of changes from a primary server.
type StreamReader struct {
	rc io.ReadCloser
	lr io.LimitedReader
}

// Close closes the underlying reader.
func (r *StreamReader) Close() (err error) {
	if e := r.rc.Close(); err == nil {
		err = e
	}
	return err
}

// NextFrame returns the frame from the underlying reader.. This call will block
// until a record is available. After calling NextFrame(), the frame payload can
// be read by calling Read() until io.EOF is reached.
func (r *StreamReader) NextFrame() (litefs.StreamFrame, error) {
	// If bytes remain on the current file, discard.
	if r.lr.N > 0 {
		if _, err := io.Copy(io.Discard, &r.lr); err != nil {
			return nil, err
		}
	}

	frame, err := litefs.ReadStreamFrame(r.rc)
	if err != nil {
		return nil, err
	}

	// Limit body to the size from the frame data.
	switch frame := frame.(type) {
	case *litefs.LTXStreamFrame:
		r.lr.N = frame.Size
	}

	return frame, nil

}

// Read reads bytes of the current payload into p. Only valid after a successful
// call to Next(). On io.EOF, call Next() again to begin reading next record.
func (r *StreamReader) Read(p []byte) (n int, err error) {
	return r.lr.Read(p)
}
