package mock

import (
	"context"
	"io"

	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
)

var _ litefs.Client = (*Client)(nil)

type Client struct {
	AcquireHaltLockFunc func(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) (*litefs.HaltLock, error)
	ReleaseHaltLockFunc func(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) error
	CommitFunc          func(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64, r io.Reader) error
	StreamFunc          func(ctx context.Context, primaryURL string, nodeID uint64, posMap map[string]ltx.Pos, filter []string) (litefs.Stream, error)
}

func (c *Client) AcquireHaltLock(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) (*litefs.HaltLock, error) {
	return c.AcquireHaltLockFunc(ctx, primaryURL, nodeID, name, lockID)
}

func (c *Client) ReleaseHaltLock(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) error {
	return c.ReleaseHaltLockFunc(ctx, primaryURL, nodeID, name, lockID)
}

func (c *Client) Commit(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64, r io.Reader) error {
	return c.CommitFunc(ctx, primaryURL, nodeID, name, lockID, r)
}

func (c *Client) Stream(ctx context.Context, primaryURL string, nodeID uint64, posMap map[string]ltx.Pos, filter []string) (litefs.Stream, error) {
	return c.StreamFunc(ctx, primaryURL, nodeID, posMap, filter)
}

type Stream struct {
	io.ReadCloser
	ClusterIDFunc func() string
}

func (s *Stream) ClusterID() string { return s.ClusterIDFunc() }
