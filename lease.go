package litefs

import (
	"context"
	"io"
	"time"
)

// Leaser represents an API for obtaining a lease for leader election.
type Leaser interface {
	io.Closer

	AdvertiseURL() string

	// Acquire attempts to acquire the lease to become the primary.
	Acquire(ctx context.Context) (Lease, error)

	// PrimaryURL attempts to read the current primary URL.
	// Returns ErrNoPrimary if no primary has the lease.
	PrimaryURL(ctx context.Context) (string, error)
}

// Lease represents an acquired lease from a Leaser.
type Lease interface {
	RenewedAt() time.Time
	TTL() time.Duration

	// Renew attempts to reset the TTL on the lease.
	// Returns ErrLeaseExpired if the lease has expired or was deleted.
	Renew(ctx context.Context) error

	// Close attempts to remove the lease from the server.
	Close() error
}

// StaticLeaser always returns a lease to a static primary.
type StaticLeaser struct {
	isPrimary  bool
	primaryURL string
}

// NewStaticLeaser returns a new instance of StaticLeaser.
func NewStaticLeaser(isPrimary bool, primaryURL string) *StaticLeaser {
	return &StaticLeaser{
		isPrimary:  isPrimary,
		primaryURL: primaryURL,
	}
}

// Close is a no-op.
func (l *StaticLeaser) Close() (err error) { return nil }

// AdvertiseURL returns the primary URL if this is the primary.
// Otherwise returns blank.
func (l *StaticLeaser) AdvertiseURL() string {
	if l.isPrimary {
		return l.primaryURL
	}
	return ""
}

// Acquire returns a lease if this node is the static primary.
// Otherwise returns ErrPrimaryExists.
func (l *StaticLeaser) Acquire(ctx context.Context) (Lease, error) {
	if !l.isPrimary {
		return nil, ErrPrimaryExists
	}
	return &StaticLease{leaser: l}, nil
}

// PrimaryURL returns the primary's URL for replicas.
// Returns ErrNoPrimary if the node is the primary.
func (l *StaticLeaser) PrimaryURL(ctx context.Context) (string, error) {
	if l.isPrimary {
		return "", ErrNoPrimary
	}
	return l.primaryURL, nil
}

// IsPrimary returns true if the current node is the primary.
func (l *StaticLeaser) IsPrimary() bool {
	return l.isPrimary
}

var _ Lease = (*StaticLease)(nil)

// StaticLease represents a lease for a fixed primary.
type StaticLease struct {
	leaser *StaticLeaser
}

// RenewedAt returns the Unix epoch in UTC.
func (l *StaticLease) RenewedAt() time.Time { return time.Unix(0, 0).UTC() }

// TTL returns the duration until the lease expires which is a time well into the future.
func (l *StaticLease) TTL() time.Duration { return staticLeaseExpiresAt.Sub(l.RenewedAt()) }

// Renew is a no-op.
func (l *StaticLease) Renew(ctx context.Context) error { return nil }

func (l *StaticLease) Close() error { return nil }

var staticLeaseExpiresAt = time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
