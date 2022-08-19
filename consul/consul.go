package consul

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"path"
	"strings"
	"time"

	"github.com/hashicorp/consul/api"
	"github.com/superfly/litefs"
)

// Default lease settings.
const (
	DefaultSessionName = "litefs"
	DefaultKey         = "litefs/primary"
	DefaultTTL         = 10 * time.Second
	DefaultLockDelay   = 5 * time.Second
)

// Leaser represents an API for obtaining a distributed lock on a single key.
type Leaser struct {
	consulURL    string
	hostname     string
	advertiseURL string
	client       *api.Client

	// SessionName is the name associated with the Consul session.
	SessionName string

	// Key is the Consul KV key use to acquire the lock.
	Key string

	// Prefix that is prepended to the key. Automatically set if the URL contains a path.
	KeyPrefix string

	// TTL is the time until the lease expires.
	TTL time.Duration

	// LockDefault is the time after the lock expires that a new lock can be acquired.
	LockDelay time.Duration
}

// NewLeaser returns a new instance of Leaser.
func NewLeaser(consulURL, hostname, advertiseURL string) *Leaser {
	return &Leaser{
		consulURL:    consulURL,
		hostname:     hostname,
		advertiseURL: advertiseURL,
		SessionName:  DefaultSessionName,
		Key:          DefaultKey,
		TTL:          DefaultTTL,
		LockDelay:    DefaultLockDelay,
	}
}

// Open initializes the Consul client.
func (l *Leaser) Open() error {
	u, err := url.Parse(l.consulURL)
	if err != nil {
		return err
	}

	if l.hostname == "" {
		return fmt.Errorf("must specify a hostname for this node")
	} else if l.advertiseURL == "" {
		return fmt.Errorf("must specify an advertise URL for this node")
	}

	config := api.DefaultConfig()
	config.HttpClient = http.DefaultClient
	config.Address = u.Host
	config.Scheme = u.Scheme
	if u.User != nil {
		config.Token, _ = u.User.Password()
	}
	if v := strings.TrimPrefix(u.Path, "/"); v != "" {
		l.KeyPrefix = v
	}

	if l.client, err = api.NewClient(config); err != nil {
		return err
	}

	// Register a node that is shared by all instances.
	if nodeName := l.NodeName(); nodeName != "" {
		if _, err := l.client.Catalog().Register(&api.CatalogRegistration{
			Node:    nodeName,
			Address: "localhost", // not used
		}, nil); err != nil {
			return fmt.Errorf("register node %q: %w", nodeName, err)
		}
	}

	return nil
}

// Close closes the underlying client.
func (l *Leaser) Close() (err error) {
	return nil
}

// Hostname returns the hostname for this node.
func (l *Leaser) Hostname() string {
	return l.hostname
}

// AdvertiseURL returns the URL being advertised to nodes when primary.
func (l *Leaser) AdvertiseURL() string {
	return l.advertiseURL
}

// NodeName returns a name for a node based on the key prefix.
func (l *Leaser) NodeName() string {
	if l.KeyPrefix == "" {
		return ""
	}
	return path.Join(l.KeyPrefix, "litefs")
}

// Acquire acquires a lock on the key and sets the value.
// Returns an error if the lease could not be obtained.
func (l *Leaser) Acquire(ctx context.Context) (_ litefs.Lease, retErr error) {
	// Create session first.
	sessionID, _, err := l.client.Session().CreateNoChecks(&api.SessionEntry{
		Node:      l.NodeName(),
		Name:      l.SessionName,
		Behavior:  "delete",
		LockDelay: l.LockDelay,
		TTL:       l.TTL.String(),
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("create consul session: %w", err)
	}
	lease := newLease(l, sessionID, time.Now())

	// Attempt to clean up session. It'll be removed via TTL eventually anyway though.
	defer func() {
		if retErr != nil {
			_ = lease.Close()
		}
	}()

	// Marshal information about the primary node.
	value, err := json.Marshal(litefs.PrimaryInfo{
		Hostname:     l.hostname,
		AdvertiseURL: l.advertiseURL,
	})
	if err != nil {
		return nil, fmt.Errorf("marshal lease info: %w", err)
	}

	// Set key with lock on session.
	acquired, _, err := l.client.KV().Acquire(&api.KVPair{
		Key:     path.Join(l.KeyPrefix, l.Key),
		Value:   value,
		Session: sessionID,
	}, nil)
	if err != nil {
		return nil, fmt.Errorf("put consul key/value: %w", err)
	} else if !acquired {
		return nil, litefs.ErrPrimaryExists
	}
	return lease, nil
}

// AcquireExisting acquires a lock using an existing session ID. This can occur
// if an existing primary hands off to a replica. Returns an error if the lease
// could not be renewed.
func (l *Leaser) AcquireExisting(ctx context.Context, leaseID string) (litefs.Lease, error) {
	lease := newLease(l, leaseID, time.Now())
	if err := lease.Renew(ctx); err != nil {
		return nil, err
	}
	return lease, nil
}

// PrimaryInfo attempts to return the current primary URL.
func (l *Leaser) PrimaryInfo(ctx context.Context) (info litefs.PrimaryInfo, err error) {
	kv, _, err := l.client.KV().Get(path.Join(l.KeyPrefix, l.Key), nil)
	if err != nil {
		return info, err
	} else if kv == nil {
		return info, litefs.ErrNoPrimary
	}

	if err := json.Unmarshal(kv.Value, &info); err != nil {
		return info, err
	}
	return info, nil
}

// Lease represents a distributed lock obtained by the Leaser.
type Lease struct {
	leaser    *Leaser
	sessionID string
	renewedAt time.Time
}

func newLease(leaser *Leaser, sessionID string, renewedAt time.Time) *Lease {
	return &Lease{
		leaser:    leaser,
		sessionID: sessionID,
		renewedAt: renewedAt,
	}
}

// TTL returns the time-to-live value the lease was initialized with.
func (l *Lease) TTL() time.Duration { return l.leaser.TTL }

// RenewedAt returns the time that the lease was created or renewed.
func (l *Lease) RenewedAt() time.Time { return l.renewedAt }

// Renew attempts to reset the TTL on the lease by renewing it.
// Returns ErrLeaseExpired if lease no longer exists.
func (l *Lease) Renew(ctx context.Context) error {
	entry, _, err := l.leaser.client.Session().Renew(l.sessionID, nil)
	if err != nil {
		return err
	} else if entry == nil {
		return litefs.ErrLeaseExpired
	}

	// Reset the last renewed time.
	l.renewedAt = time.Now()
	return nil
}

// Close destroys the underlying session.
func (l *Lease) Close() error {
	_, err := l.leaser.client.Session().Destroy(l.sessionID, nil)
	return err
}
