package mock

import (
	"context"
	"io"

	"github.com/superfly/litefs"
)

type Client struct {
	StreamFunc func(ctx context.Context, rawurl string, id string, posMap map[uint32]litefs.Pos) (io.ReadCloser, error)
}

func (c *Client) Stream(ctx context.Context, rawurl string, id string, posMap map[uint32]litefs.Pos) (io.ReadCloser, error) {
	return c.StreamFunc(ctx, rawurl, id, posMap)
}
