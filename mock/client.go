package mock

import (
	"context"
	"io"

	"github.com/superfly/litefs"
)

var _ litefs.Client = (*Client)(nil)

type Client struct {
	BeginFunc  func(ctx context.Context, primaryURL string, nodeID, name string) (litefs.Tx, error)
	StreamFunc func(ctx context.Context, primaryURL string, nodeID string, posMap map[string]litefs.Pos) (io.ReadCloser, error)
}

func (c *Client) Begin(ctx context.Context, primaryURL string, nodeID, name string) (litefs.Tx, error) {
	return c.BeginFunc(ctx, primaryURL, nodeID, name)
}

func (c *Client) Stream(ctx context.Context, primaryURL string, id string, posMap map[string]litefs.Pos) (io.ReadCloser, error) {
	return c.StreamFunc(ctx, primaryURL, id, posMap)
}

var _ litefs.Tx = (*Tx)(nil)

type Tx struct {
	IDFunc               func() uint64
	PreApplyChecksumFunc func() uint64

	CommitFunc   func(r io.Reader) error
	RollbackFunc func() error
}

func (tx *Tx) ID() uint64               { return tx.IDFunc() }
func (tx *Tx) PreApplyChecksum() uint64 { return tx.PreApplyChecksumFunc() }

func (tx *Tx) Commit(r io.Reader) error { return tx.CommitFunc(r) }
func (tx *Tx) Rollback() error          { return tx.RollbackFunc() }
