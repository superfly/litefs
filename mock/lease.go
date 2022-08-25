package mock

import (
	"context"
	"time"

	"github.com/superfly/litefs"
)

var _ litefs.Leaser = (*Leaser)(nil)

type Leaser struct {
	CloseFunc        func() error
	AdvertiseURLFunc func() string
	AcquireFunc      func(ctx context.Context) (litefs.Lease, error)
	PrimaryInfoFunc  func(ctx context.Context) (litefs.PrimaryInfo, error)
}

func (l *Leaser) Close() error {
	return l.CloseFunc()
}

func (l *Leaser) AdvertiseURL() string {
	return l.AdvertiseURLFunc()
}

func (l *Leaser) Acquire(ctx context.Context) (litefs.Lease, error) {
	return l.AcquireFunc(ctx)
}

func (l *Leaser) PrimaryInfo(ctx context.Context) (litefs.PrimaryInfo, error) {
	return l.PrimaryInfoFunc(ctx)
}

var _ litefs.Lease = (*Lease)(nil)

type Lease struct {
	RenewedAtFunc func() time.Time
	TTLFunc       func() time.Duration
	RenewFunc     func(ctx context.Context) error
	CloseFunc     func() error
}

func (l *Lease) RenewedAt() time.Time {
	return l.RenewedAtFunc()
}

func (l *Lease) TTL() time.Duration {
	return l.TTLFunc()
}

func (l *Lease) Renew(ctx context.Context) error {
	return l.RenewFunc(ctx)
}

func (l *Lease) Close() error {
	return l.CloseFunc()
}
