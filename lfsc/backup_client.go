package lfsc

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
)

var _ litefs.BackupClient = (*BackupClient)(nil)

// BackupClient implements a backup client for LiteFS Cloud.
type BackupClient struct {
	store   *litefs.Store // store, used for cluster ID
	baseURL url.URL       // remote LiteFS Cloud URL
	cluster string        // name of cluster

	// Authentication fields passed in via the "Authorization" HTTP header.
	AuthScheme string
	AuthToken  string

	HTTPClient *http.Client
}

// NewBackupClient returns a new instance of BackupClient.
func NewBackupClient(store *litefs.Store, u url.URL, cluster string) *BackupClient {
	return &BackupClient{
		store: store,
		baseURL: url.URL{
			Scheme: u.Scheme,
			Host:   u.Host,
		},
		cluster: cluster,

		HTTPClient: &http.Client{},
	}
}

// Open validates the URL the client was initialized with.
func (c *BackupClient) Open() (err error) {
	if c.baseURL.Scheme != "http" && c.baseURL.Scheme != "https" {
		return fmt.Errorf("invalid litefs cloud URL scheme: %q", c.baseURL.Scheme)
	} else if c.baseURL.Host == "" {
		return fmt.Errorf("litefs cloud URL host required: %q", c.baseURL.String())
	}

	if c.cluster == "" {
		return fmt.Errorf("cluster name required")
	}

	return nil
}

// URL of the backup service.
func (c *BackupClient) URL() string {
	return c.baseURL.String()
}

// Cluster returns the name of the cluster that the client was initialized with.
func (c *BackupClient) Cluster() string {
	return c.cluster
}

// PosMap returns the replication position for all databases on the backup service.
func (c *BackupClient) PosMap(ctx context.Context) (map[string]ltx.Pos, error) {
	req, err := c.newRequest(http.MethodGet, "/pos", url.Values{"cluster": {c.cluster}}, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	m := make(map[string]ltx.Pos)
	if err := json.NewDecoder(resp.Body).Decode(&m); err != nil {
		return nil, err
	}
	return m, nil
}

// WriteTx writes an LTX file to the backup service. The file must be
// contiguous with the latest LTX file on the backup service or else it
// will return an ltx.PosMismatchError.
func (c *BackupClient) WriteTx(ctx context.Context, name string, r io.Reader) (hwm ltx.TXID, err error) {
	req, err := c.newRequest(http.MethodPost, "/db/tx", url.Values{
		"cluster": {c.cluster},
		"db":      {name},
	}, r)
	if err != nil {
		return 0, err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return 0, err
	} else if err := resp.Body.Close(); err != nil {
		return 0, err
	}

	// Parse high-water mark returned from server.
	hwmStr := resp.Header.Get("Litefs-Hwm")
	if hwm, err = ltx.ParseTXID(hwmStr); err != nil {
		return 0, fmt.Errorf("cannot parse high-water mark: %q", hwmStr)
	}

	return hwm, nil
}

// FetchSnapshot requests a full snapshot of the database as it exists on
// the backup service. This should be used if the LiteFS node has become
// out of sync with the backup service.
func (c *BackupClient) FetchSnapshot(ctx context.Context, name string) (io.ReadCloser, error) {
	req, err := c.newRequest(http.MethodGet, "/db/snapshot", url.Values{
		"cluster": {c.cluster},
		"db":      {name},
	}, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.doRequest(ctx, req)
	if err != nil {
		return nil, err
	}
	return resp.Body, nil
}

// newRequest returns a new HTTP request with the given context & auth parameters.
func (c *BackupClient) newRequest(method, path string, q url.Values, body io.Reader) (*http.Request, error) {
	u := c.baseURL
	u.Path = path
	u.RawQuery = q.Encode()

	req, err := http.NewRequest(method, u.String(), body)
	if err != nil {
		return nil, err
	}

	// Send the cluster ID with every request.
	if clusterID := c.store.ClusterID(); clusterID != "" {
		req.Header.Set("Litefs-Cluster-Id", clusterID)
	}

	// Set the auth header if scheme & token are provided. Otherwise send without auth.
	if c.AuthScheme != "" && c.AuthToken != "" {
		req.Header.Set("Authorization", fmt.Sprintf("%s %s", c.AuthScheme, c.AuthToken))
	}
	return req, nil
}

// doRequest executes the request and returns an error if the response is not a 2XX.
func (c *BackupClient) doRequest(ctx context.Context, req *http.Request) (*http.Response, error) {
	resp, err := c.HTTPClient.Do(req.WithContext(ctx))
	if err != nil {
		return nil, err
	}

	// If this is not a 2XX code then read the body as an error message.
	if !isSuccessfulStatusCode(resp.StatusCode) {
		return nil, readResponseError(resp)
	}

	return resp, nil
}

// readResponseError reads the response body as an error message & closes the body.
func readResponseError(resp *http.Response) error {
	defer func() { _ = resp.Body.Close() }()

	// Read up to 64KB of data from the body for the error message.
	buf, err := io.ReadAll(io.LimitReader(resp.Body, 1<<16))
	if err != nil {
		return err
	}

	// Attempt to decode as a JSON error.
	var e errorResponse
	if err := json.Unmarshal(buf, &e); err != nil {
		return fmt.Errorf("backup client error (%d): %s", resp.StatusCode, string(buf))
	}

	// Match specific types of errors.
	switch e.Code {
	case "EPOSMISMATCH":
		return ltx.NewPosMismatchError(e.Pos)
	default:
		return fmt.Errorf("backup client error (%d): %s", resp.StatusCode, e.Error)
	}
}

type errorResponse struct {
	Code  string  `json:"code"`
	Error string  `json:"error"`
	Pos   ltx.Pos `json:"pos"`
}

func isSuccessfulStatusCode(code int) bool {
	return code >= 200 && code < 300
}
