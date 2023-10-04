package litefs

import (
	"context"
	"fmt"
	"io"
	"time"
)

// Leaser represents an API for obtaining a lease for leader election.
type Leaser interface {
	io.Closer

	// Type returns the name of the leaser.
	Type() string

	Hostname() string
	AdvertiseURL() string

	// Acquire attempts to acquire the lease to become the primary.
	Acquire(ctx context.Context) (Lease, error)

	// AcquireExisting returns a lease from an existing lease ID.
	// This occurs when the primary is handed off to a replica node.
	AcquireExisting(ctx context.Context, leaseID string) (Lease, error)

	// PrimaryInfo attempts to read the current primary data.
	// Returns ErrNoPrimary if no primary currently has the lease.
	PrimaryInfo(ctx context.Context) (PrimaryInfo, error)

	// ClusterID returns the cluster ID set on the leaser.
	// This is used to ensure two clusters do not accidentally overlap.
	ClusterID(ctx context.Context) (string, error)

	// SetClusterID sets the cluster ID on the leaser.
	SetClusterID(ctx context.Context, clusterID string) error
}

// Lease represents an acquired lease from a Leaser.
type Lease interface {
	ID() string
	RenewedAt() time.Time
	TTL() time.Duration

	// Renew attempts to reset the TTL on the lease.
	// Returns ErrLeaseExpired if the lease has expired or was deleted.
	Renew(ctx context.Context) error

	// Marks the lease as handed-off to another node.
	// This should send the nodeID to the channel returned by HandoffCh().
	Handoff(ctx context.Context, nodeID uint64) error
	HandoffCh() <-chan uint64

	// Close attempts to remove the lease from the server.
	Close() error
}

// PrimaryInfo is the JSON object stored in the Consul lease value.
type PrimaryInfo struct {
	Hostname     string `json:"hostname"`
	AdvertiseURL string `json:"advertise-url"`
}

// Clone returns a copy of info.
func (info *PrimaryInfo) Clone() *PrimaryInfo {
	if info == nil {
		return nil
	}
	other := *info
	return &other
}

// StaticLeaser always returns a lease to a static primary.
type StaticLeaser struct {
	isPrimary    bool
	hostname     string
	advertiseURL string
}

// NewStaticLeaser returns a new instance of StaticLeaser.
func NewStaticLeaser(isPrimary bool, hostname, advertiseURL string) *StaticLeaser {
	return &StaticLeaser{
		isPrimary:    isPrimary,
		hostname:     hostname,
		advertiseURL: advertiseURL,
	}
}

// Close is a no-op.
func (l *StaticLeaser) Close() (err error) { return nil }

// Type returns "static".
func (l *StaticLeaser) Type() string { return "static" }

func (l *StaticLeaser) Hostname() string {
	return l.hostname
}

// AdvertiseURL returns the primary URL if this is the primary.
// Otherwise returns blank.
func (l *StaticLeaser) AdvertiseURL() string {
	if l.isPrimary {
		return l.advertiseURL
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

// AcquireExisting always returns an error. Static leasing does not support handoff.
func (l *StaticLeaser) AcquireExisting(ctx context.Context, leaseID string) (Lease, error) {
	return nil, fmt.Errorf("static lease handoff not supported")
}

// PrimaryInfo returns the primary's info.
// Returns ErrNoPrimary if the node is the primary.
func (l *StaticLeaser) PrimaryInfo(ctx context.Context) (PrimaryInfo, error) {
	if l.isPrimary {
		return PrimaryInfo{}, ErrNoPrimary
	}
	return PrimaryInfo{
		Hostname:     l.hostname,
		AdvertiseURL: l.advertiseURL,
	}, nil
}

// IsPrimary returns true if the current node is the primary.
func (l *StaticLeaser) IsPrimary() bool {
	return l.isPrimary
}

// ClusterID always returns a blank string for the static leaser.
func (l *StaticLeaser) ClusterID(ctx context.Context) (string, error) {
	return "", nil
}

// SetClusterID is always a no-op for the static leaser.
func (l *StaticLeaser) SetClusterID(ctx context.Context, clusterID string) error {
	return nil
}

var _ Lease = (*StaticLease)(nil)

// StaticLease represents a lease for a fixed primary.
type StaticLease struct {
	leaser *StaticLeaser
}

// ID always returns a blank string.
func (l *StaticLease) ID() string { return "" }

// RenewedAt returns the Unix epoch in UTC.
func (l *StaticLease) RenewedAt() time.Time { return time.Unix(0, 0).UTC() }

// TTL returns the duration until the lease expires which is a time well into the future.
func (l *StaticLease) TTL() time.Duration { return staticLeaseExpiresAt.Sub(l.RenewedAt()) }

// Renew is a no-op.
func (l *StaticLease) Renew(ctx context.Context) error { return nil }

// Handoff always returns an error.
func (l *StaticLease) Handoff(ctx context.Context, nodeID uint64) error {
	return fmt.Errorf("static lease does not support handoff")
}

// HandoffCh always returns a nil channel.
func (l *StaticLease) HandoffCh() <-chan uint64 { return nil }

func (l *StaticLease) Close() error { return nil }

var staticLeaseExpiresAt = time.Date(3000, time.January, 1, 0, 0, 0, 0, time.UTC)
