package litefs

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/binary"
	"encoding/json"
	"errors"
	"expvar"
	"fmt"
	"io"
	"log"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/litefs/internal/chunk"
	"github.com/superfly/ltx"
	"golang.org/x/exp/slog"
	"golang.org/x/sync/errgroup"
)

// Default store settings.
const (
	DefaultReconnectDelay = 1 * time.Second
	DefaultDemoteDelay    = 10 * time.Second

	DefaultRetention                = 10 * time.Minute
	DefaultRetentionMonitorInterval = 1 * time.Minute

	DefaultHaltAcquireTimeout      = 10 * time.Second
	DefaultHaltLockTTL             = 30 * time.Second
	DefaultHaltLockMonitorInterval = 5 * time.Second

	DefaultBackupDelay            = 1 * time.Second
	DefaultBackupFullSyncInterval = 10 * time.Second
)

const (
	// MaxBackupLTXFileN is the number of LTX files that can be compacted
	// together at a time when sending data to the backup service.
	MaxBackupLTXFileN = 256

	MetricsMonitorInterval = 1 * time.Second
)

var ErrStoreClosed = fmt.Errorf("store closed")

// GlobalStore represents a single store used for metrics collection.
var GlobalStore atomic.Value

// Store represents a collection of databases.
type Store struct {
	mu   sync.Mutex
	path string

	id                   uint64 // unique node id
	clusterID            atomic.Value
	dbs                  map[string]*DB
	changeSetSubscribers map[*ChangeSetSubscriber]struct{}
	eventSubscribers     map[*EventSubscriber]struct{}
	primaryTimestamp     atomic.Int64 // ms since epoch of last update from primary. -1 if primary

	lease       Lease         // if not nil, store is current primary
	primaryCh   chan struct{} // closed when primary loses leadership
	primaryInfo *PrimaryInfo  // contains info about the current primary
	candidate   bool          // if true, we are eligible to become the primary
	readyCh     chan struct{} // closed when primary found or acquired
	demoteCh    chan struct{} // closed when Demote() is called

	ctx    context.Context
	cancel context.CancelCauseFunc
	g      errgroup.Group

	// The operating system interface to use for system calls. Defaults to SystemOS.
	OS   OS
	Exit func(int)

	// Client used to connect to other LiteFS instances.
	Client Client

	// Leaser manages the lease that controls leader election.
	Leaser Leaser

	// BackupClient is the client to connect to an external backup service.
	BackupClient BackupClient

	// If true, LTX files are compressed using LZ4.
	Compress bool

	// Time to wait after disconnecting from the primary to reconnect.
	ReconnectDelay time.Duration

	// Time to wait after manually demoting trying to become primary again.
	DemoteDelay time.Duration

	// Length of time to retain LTX files.
	Retention                time.Duration
	RetentionMonitorInterval time.Duration

	// Max time to hold HALT lock and interval between expiration checks.
	HaltLockTTL             time.Duration
	HaltLockMonitorInterval time.Duration

	// Time to wait to acquire the HALT lock.
	HaltAcquireTimeout time.Duration

	// Time after a change is made before it is sent to the backup service.
	// This allows multiple changes in quick succession to be batched together.
	BackupDelay time.Duration

	// Interval between checks to re-fetch the position map. This ensures that
	// restores on the backup server are detected by the LiteFS primary.
	BackupFullSyncInterval time.Duration

	// Callback to notify kernel of file changes.
	Invalidator Invalidator

	// Interface to interact with the host environment.
	Environment Environment

	// Specifies a subset of databases to replicate from the primary.
	DatabaseFilter []string

	// If true, computes and verifies the checksum of the entire database
	// after every transaction. Should only be used during testing.
	StrictVerify bool
}

// NewStore returns a new instance of Store.
func NewStore(path string, candidate bool) *Store {
	primaryCh := make(chan struct{})
	close(primaryCh)

	// Generate random node ID to prevent connecting to itself.
	b := make([]byte, 16)
	if _, err := io.ReadFull(crand.Reader, b); err != nil {
		panic(fmt.Errorf("cannot generate node id: %w", err))
	}

	s := &Store{
		id:   binary.BigEndian.Uint64(b),
		path: path,

		dbs: make(map[string]*DB),

		changeSetSubscribers: make(map[*ChangeSetSubscriber]struct{}),
		eventSubscribers:     make(map[*EventSubscriber]struct{}),

		candidate: candidate,
		primaryCh: primaryCh,
		readyCh:   make(chan struct{}),
		demoteCh:  make(chan struct{}),

		OS:   &internal.SystemOS{},
		Exit: os.Exit,

		ReconnectDelay: DefaultReconnectDelay,
		DemoteDelay:    DefaultDemoteDelay,

		Retention:                DefaultRetention,
		RetentionMonitorInterval: DefaultRetentionMonitorInterval,

		HaltAcquireTimeout:      DefaultHaltAcquireTimeout,
		HaltLockTTL:             DefaultHaltLockTTL,
		HaltLockMonitorInterval: DefaultHaltLockMonitorInterval,

		BackupDelay:            DefaultBackupDelay,
		BackupFullSyncInterval: DefaultBackupFullSyncInterval,

		Environment: &nopEnvironment{},
	}
	s.ctx, s.cancel = context.WithCancelCause(context.Background())
	s.clusterID.Store("")
	s.primaryTimestamp.Store(-1)

	return s
}

// Path returns underlying data directory.
func (s *Store) Path() string { return s.path }

// DBDir returns the folder that stores all databases.
func (s *Store) DBDir() string {
	return filepath.Join(s.path, "dbs")
}

// DBPath returns the folder that stores a single database.
func (s *Store) DBPath(name string) string {
	return filepath.Join(s.path, "dbs", name)
}

// ClusterIDPath returns the filename where the cluster ID is stored.
func (s *Store) ClusterIDPath() string {
	return filepath.Join(s.path, "clusterid")
}

// ID returns the unique identifier for this instance. Available after Open().
// Persistent across restarts if underlying storage is persistent.
func (s *Store) ID() uint64 {
	return s.id
}

// ClusterID returns the cluster ID.
func (s *Store) ClusterID() string {
	return s.clusterID.Load().(string)
}

// setClusterID saves the cluster ID to disk.
func (s *Store) setClusterID(id string) error {
	if s.ClusterID() == id {
		return nil // no-op
	}

	if err := ValidateClusterID(id); err != nil {
		return err
	}

	filename := s.ClusterIDPath()
	tempFilename := filename + ".tmp"
	defer func() { _ = s.OS.Remove("SETCLUSTERID", tempFilename) }()

	if err := s.OS.MkdirAll("SETCLUSTERID", filepath.Dir(filename), 0o777); err != nil {
		return err
	}

	f, err := s.OS.Create("SETCLUSTERID", tempFilename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := io.WriteString(f, id+"\n"); err != nil {
		return err
	}

	if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	if err := s.OS.Rename("SETCLUSTERID", tempFilename, filename); err != nil {
		return err
	} else if err := internal.Sync(filepath.Dir(filename)); err != nil {
		return err
	}

	s.clusterID.Store(id)
	return nil
}

// Open initializes the store based on files in the data directory.
func (s *Store) Open() error {
	if s.Leaser == nil {
		return fmt.Errorf("leaser required")
	}

	if err := s.OS.MkdirAll("OPEN", s.path, 0o777); err != nil {
		return err
	}

	// Attempt to remove persisted node ID from disk.
	// See: https://github.com/superfly/litefs/issues/361
	_ = s.OS.Remove("OPEN:ID", filepath.Join(s.path, "id"))

	// Load cluster ID from disk, if available locally.
	if err := s.readClusterID(); err != nil {
		return fmt.Errorf("load cluster id: %w", err)
	}

	if err := s.openDatabases(); err != nil {
		return fmt.Errorf("open databases: %w", err)
	}

	// Begin background replication monitor.
	s.g.Go(func() error { return s.monitorLease(s.ctx) })

	// Begin lock monitor.
	s.g.Go(func() error { return s.monitorHaltLock(s.ctx) })

	// Begin retention monitor.
	if s.RetentionMonitorInterval > 0 {
		s.g.Go(func() error { return s.monitorRetention(s.ctx) })
	}

	return nil
}

// readClusterID reads the cluster ID from the "clusterid" file.
// Skipped if no cluster id file exists.
func (s *Store) readClusterID() error {
	b, err := s.OS.ReadFile("READCLUSTERID", s.ClusterIDPath())
	if os.IsNotExist(err) {
		return nil
	} else if err != nil {
		return err
	}

	clusterID := strings.TrimSpace(string(b))
	if err := ValidateClusterID(clusterID); err != nil {
		return err
	}
	s.clusterID.Store(clusterID)

	return nil
}

func (s *Store) openDatabases() error {
	if err := s.OS.MkdirAll("OPENDATABASES", s.DBDir(), 0o777); err != nil {
		return err
	}

	fis, err := s.OS.ReadDir("OPENDATABASES", s.DBDir())
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}
	for _, fi := range fis {
		if err := s.openDatabase(fi.Name()); err != nil {
			return fmt.Errorf("open database(%q): %w", fi.Name(), err)
		}
	}

	// Update metrics.
	storeDBCountMetric.Set(float64(len(s.dbs)))

	return nil
}

func (s *Store) openDatabase(name string) error {
	// Instantiate and open database.
	db := NewDB(s, name, s.DBPath(name))
	if err := db.Open(); err != nil {
		return err
	}

	// Add to internal lookups.
	s.dbs[db.Name()] = db

	return nil
}

// Close signals for the store to shut down.
func (s *Store) Close() (retErr error) {
	s.cancel(ErrStoreClosed)
	retErr = s.g.Wait()

	// Release outstanding HALT locks.
	for _, db := range s.DBs() {
		haltLock := db.RemoteHaltLock()
		if haltLock == nil {
			continue
		}

		log.Printf("releasing halt lock on %q", db.Name())

		if err := db.ReleaseRemoteHaltLock(context.Background(), haltLock.ID); err != nil {
			log.Printf("cannot release halt lock on %q on shutdown", db.Name())
		}
	}

	return retErr
}

// ReadyCh returns a channel that is closed once the store has become primary
// or once it has connected to the primary.
func (s *Store) ReadyCh() chan struct{} {
	return s.readyCh
}

func (s *Store) isReady() bool {
	select {
	case <-s.readyCh:
		return true
	default:
		return false
	}
}

// markReady closes the ready channel if it hasn't already been closed.
func (s *Store) markReady() {
	select {
	case <-s.readyCh:
		return
	default:
		close(s.readyCh)
	}
}

// Demote instructs store to destroy its primary lease, if any.
// Store will wait momentarily before attempting to become primary again.
func (s *Store) Demote() {
	s.mu.Lock()
	defer s.mu.Unlock()
	close(s.demoteCh)
	s.demoteCh = make(chan struct{})
}

// Handoff instructs store to send its lease to a connected replica.
func (s *Store) Handoff(ctx context.Context, nodeID uint64) error {
	var lease Lease
	if err := func() error {
		s.mu.Lock()
		defer s.mu.Unlock()

		// Ensure this node is currently the primary and has a lease.
		lease = s.lease
		if lease == nil {
			return fmt.Errorf("node is not currently primary")
		}

		// Find connected subscriber by node ID.
		sub := s.changeSetSubscriberByNodeID(nodeID)
		if sub == nil {
			return fmt.Errorf("target node is not currently connected")
		}
		return nil
	}(); err != nil {
		return err
	}

	// Attempt to handoff the lease.
	// Not all lease systems support handoff so this may return an error.
	return lease.Handoff(ctx, nodeID)
}

// IsPrimary returns true if store has a lease to be the primary.
func (s *Store) IsPrimary() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isPrimary()
}

func (s *Store) isPrimary() bool { return s.lease != nil }

func (s *Store) setLease(lease Lease) {
	// Create a new channel to notify about primary loss when becoming primary.
	// Or close existing channel if we are losing our primary status.
	if (s.lease != nil) != (lease != nil) {
		if lease != nil {
			s.primaryCh = make(chan struct{})
			s.setPrimaryTimestamp(0)
		} else {
			close(s.primaryCh)
			s.setPrimaryTimestamp(-1)
		}
	}

	// Store current lease
	s.lease = lease

	// Update metrics.
	if s.isPrimary() {
		storeIsPrimaryMetric.Set(1)
	} else {
		storeIsPrimaryMetric.Set(0)
	}

	s.notifyPrimaryChange()
}

// PrimaryCtx wraps ctx with another context that will cancel when no longer primary.
func (s *Store) PrimaryCtx(ctx context.Context) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.primaryCtx(ctx)
}

func (s *Store) primaryCtx(ctx context.Context) context.Context {
	return newPrimaryCtx(ctx, s.primaryCh)
}

// PrimaryInfo returns info about the current primary.
func (s *Store) PrimaryInfo() (isPrimary bool, info *PrimaryInfo) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isPrimary(), s.primaryInfo.Clone()
}

// PrimaryInfoWithContext continually attempts to fetch the primary info until available.
// Returns when isPrimary is true, info is non-nil, or when ctx is done.
func (s *Store) PrimaryInfoWithContext(ctx context.Context) (isPrimary bool, info *PrimaryInfo) {
	if isPrimary, info = s.PrimaryInfo(); isPrimary || info != nil {
		return isPrimary, info
	}

	ticker := time.NewTicker(100 * time.Microsecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return isPrimary, info
		case <-ticker.C:
			if isPrimary, info = s.PrimaryInfo(); isPrimary || info != nil {
				return isPrimary, info
			}
		}
	}
}

func (s *Store) setPrimaryInfo(info *PrimaryInfo) {
	s.primaryInfo = info
	s.notifyPrimaryChange()
}

// Candidate returns true if store is eligible to be the primary.
func (s *Store) Candidate() bool {
	return s.candidate
}

// DBByName returns a database by name.
// Returns nil if the database does not exist.
func (s *Store) DB(name string) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dbs[name]
}

// DBs returns a list of databases.
func (s *Store) DBs() []*DB {
	s.mu.Lock()
	defer s.mu.Unlock()

	a := make([]*DB, 0, len(s.dbs))
	for _, db := range s.dbs {
		a = append(a, db)
	}
	return a
}

// CreateDB creates a new database with the given name. The returned file handle
// must be closed by the caller. Returns an error if a database with the same
// name already exists.
func (s *Store) CreateDB(name string) (db *DB, f *os.File, err error) {
	defer func() {
		TraceLog.Printf("[CreateDatabase(%s)]: %s", name, errorKeyValue(err))
	}()

	s.mu.Lock()
	defer s.mu.Unlock()

	// Check if the database already exists. We could have a reference to a
	// zero-length database which means it was previously deleted.
	requireNewDB := true
	if db = s.dbs[name]; db != nil {
		if db.PageN() > 0 {
			return nil, nil, ErrDatabaseExists
		}
		requireNewDB = false
	}

	// Generate database directory with name file & empty database file.
	dbPath := s.DBPath(name)
	if err := s.OS.MkdirAll("CREATDEDB", dbPath, 0o777); err != nil {
		return nil, nil, err
	}

	f, err = s.OS.OpenFile("CREATDEDB", filepath.Join(dbPath, "database"), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0o666)
	if err != nil {
		return nil, nil, err
	}

	// Create new database instance and add to maps.
	if requireNewDB {
		db = NewDB(s, name, dbPath)
		if err := db.Open(); err != nil {
			_ = f.Close()
			return nil, nil, err
		}
		s.dbs[name] = db
	}

	// Notify listeners of change.
	s.markDirty(name)

	// Update metrics
	storeDBCountMetric.Set(float64(len(s.dbs)))

	return db, f, nil
}

// CreateDBIfNotExists creates an empty database with the given name.
func (s *Store) CreateDBIfNotExists(name string) (*DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Exit if database with same name already exists.
	if db := s.dbs[name]; db != nil {
		return db, nil
	}

	// Generate database directory with name file & empty database file.
	dbPath := s.DBPath(name)
	if err := s.OS.MkdirAll("CREATDEDBIFNOTEXISTS", dbPath, 0o777); err != nil {
		return nil, err
	}

	if err := s.OS.WriteFile("CREATDEDBIFNOTEXISTS", filepath.Join(dbPath, "database"), nil, 0o666); err != nil {
		return nil, err
	}

	// Create new database instance and add to maps.
	db := NewDB(s, name, dbPath)
	if err := db.Open(); err != nil {
		return nil, err
	}
	s.dbs[name] = db

	// Notify listeners of change.
	s.markDirty(name)

	// Update metrics
	storeDBCountMetric.Set(float64(len(s.dbs)))

	return db, nil
}

// PosMap returns a map of databases and their transactional position.
func (s *Store) PosMap() map[string]ltx.Pos {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := make(map[string]ltx.Pos, len(s.dbs))
	for _, db := range s.dbs {
		m[db.Name()] = db.Pos()
	}
	return m
}

// SubscribeChangeSet creates a new subscriber for store changes.
func (s *Store) SubscribeChangeSet(nodeID uint64) *ChangeSetSubscriber {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub := newChangeSetSubscriber(s, nodeID)
	s.changeSetSubscribers[sub] = struct{}{}

	storeSubscriberCountMetric.Set(float64(len(s.changeSetSubscribers)))
	return sub
}

// UnsubscribeChangeSet removes a subscriber from the store.
func (s *Store) UnsubscribeChangeSet(sub *ChangeSetSubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.changeSetSubscribers, sub)
	storeSubscriberCountMetric.Set(float64(len(s.changeSetSubscribers)))
}

// SubscriberByNodeID returns a subscriber by node ID.
// Returns nil if the node is not currently subscribed to the store.
func (s *Store) SubscriberByNodeID(nodeID uint64) *ChangeSetSubscriber {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.changeSetSubscriberByNodeID(nodeID)
}

func (s *Store) changeSetSubscriberByNodeID(nodeID uint64) *ChangeSetSubscriber {
	for sub := range s.changeSetSubscribers {
		if sub.NodeID() == nodeID {
			return sub
		}
	}
	return nil
}

// MarkDirty marks a database dirty on all subscribers.
func (s *Store) MarkDirty(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markDirty(name)
}

func (s *Store) markDirty(name string) {
	for sub := range s.changeSetSubscribers {
		sub.MarkDirty(name)
	}
}

// SubscribeEvents creates a new subscriber for store events.
func (s *Store) SubscribeEvents() *EventSubscriber {
	s.mu.Lock()
	defer s.mu.Unlock()

	var hostname string
	if s.primaryInfo != nil {
		hostname = s.primaryInfo.Hostname
	}

	sub := newEventSubscriber(s)
	sub.ch <- Event{
		Type: EventTypeInit,
		Data: InitEventData{
			IsPrimary: s.isPrimary(),
			Hostname:  hostname,
		},
	}

	s.eventSubscribers[sub] = struct{}{}

	return sub
}

// UnsubscribeEvents removes an event subscriber from the store.
func (s *Store) UnsubscribeEvents(sub *EventSubscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unsubscribeEvents(sub)
}

func (s *Store) unsubscribeEvents(sub *EventSubscriber) {
	if _, ok := s.eventSubscribers[sub]; ok {
		delete(s.eventSubscribers, sub)
		close(sub.ch)
	}
}

// NotifyEvent sends event to all event subscribers.
// If a subscriber has no additional buffer space available then it is closed.
func (s *Store) NotifyEvent(event Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.notifyEvent(event)
}

func (s *Store) notifyEvent(event Event) {
	for sub := range s.eventSubscribers {
		select {
		case sub.ch <- event:
		default:
			s.unsubscribeEvents(sub)
		}
	}
}

func (s *Store) notifyPrimaryChange() {
	var hostname string
	if s.primaryInfo != nil {
		hostname = s.primaryInfo.Hostname
	}

	s.notifyEvent(Event{
		Type: EventTypePrimaryChange,
		Data: PrimaryChangeEventData{
			IsPrimary: s.isPrimary(),
			Hostname:  hostname,
		},
	})
}

// monitorLease continuously handles either the leader lease or replicates from the primary.
func (s *Store) monitorLease(ctx context.Context) (err error) {
	// Initialize environment to indicate this node is not a primary.
	s.Environment.SetPrimaryStatus(ctx, false)

	var handoffLeaseID string
	for {
		// Exit if store is closed.
		if err := ctx.Err(); err != nil {
			return nil
		}

		// If a cluster ID exists on the server, ensure it matches what we have.
		var info PrimaryInfo
		if leaserClusterID, err := s.Leaser.ClusterID(ctx); err != nil {
			log.Printf("cannot fetch cluster ID from %q lease, retrying: %s", s.Leaser.Type(), err)
			sleepWithContext(ctx, s.ReconnectDelay)
			continue

		} else if leaserClusterID != "" && s.ClusterID() != "" && leaserClusterID != s.ClusterID() {
			log.Printf("cannot connect, %q lease already initialized with different ID: %s", s.Leaser.Type(), leaserClusterID)
			sleepWithContext(ctx, s.ReconnectDelay)
			continue

		} else if leaserClusterID != "" && s.ClusterID() == "" {
			log.Printf("cannot become primary, local node has no cluster ID and %q lease already initialized with cluster ID %s", s.Leaser.Type(), leaserClusterID)

			if info, err = s.Leaser.PrimaryInfo(ctx); err != nil {
				log.Printf("cannot find primary, retrying: %s", err)
				sleepWithContext(ctx, s.ReconnectDelay)
				continue
			}

		} else {
			// At this point, either the leaser has a cluster ID and ours matches,
			// or the leaser has no cluster ID. We'll update the leaser once we
			// become primary.

			// If we have been handed a lease ID from the current primary, use that
			// and act like we're the new primary.
			var lease Lease
			if handoffLeaseID != "" {
				// Move lease to a local variable so we can clear the outer scope.
				leaseID := handoffLeaseID
				handoffLeaseID = ""

				// We'll only try to acquire the lease once. If it fails, then it
				// reverts back to the regular primary/replica flow.
				log.Printf("%s: acquiring existing lease from handoff", FormatNodeID(s.id))
				if lease, err = s.Leaser.AcquireExisting(ctx, leaseID); err != nil {
					log.Printf("%s: cannot acquire existing lease from handoff, retrying: %s", FormatNodeID(s.id), err)
					sleepWithContext(ctx, s.ReconnectDelay)
					continue
				}

			} else {
				// Otherwise, attempt to either obtain a primary lock or read the current primary.
				lease, info, err = s.acquireLeaseOrPrimaryInfo(ctx)
				if err == ErrNoPrimary && !s.candidate {
					log.Printf("%s: cannot find primary & ineligible to become primary, retrying: %s", FormatNodeID(s.id), err)
					sleepWithContext(ctx, s.ReconnectDelay)
					continue
				} else if err != nil {
					log.Printf("%s: cannot acquire lease or find primary, retrying: %s", FormatNodeID(s.id), err)
					sleepWithContext(ctx, s.ReconnectDelay)
					continue
				}
			}

			// Monitor as primary if we have obtained a lease.
			if lease != nil {
				log.Printf("%s: primary lease acquired, advertising as %s", FormatNodeID(s.id), s.Leaser.AdvertiseURL())
				if err := s.monitorLeaseAsPrimary(ctx, lease); err != nil {
					log.Printf("%s: primary lease lost, retrying: %s", FormatNodeID(s.id), err)
				}
				if err := s.Recover(ctx); err != nil {
					log.Printf("%s: state change recovery error (primary): %s", FormatNodeID(s.id), err)
				}
				continue
			}
		}

		// Monitor as replica if another primary already exists.
		log.Printf("%s: existing primary found (%s), connecting as replica to %q", FormatNodeID(s.id), info.Hostname, info.AdvertiseURL)
		if handoffLeaseID, err = s.monitorLeaseAsReplica(ctx, info); err == nil {
			log.Printf("%s: disconnected from primary, retrying", FormatNodeID(s.id))
		} else {
			log.Printf("%s: disconnected from primary with error, retrying: %s", FormatNodeID(s.id), err)
		}
		if err := s.Recover(ctx); err != nil {
			log.Printf("%s: state change recovery error (replica): %s", FormatNodeID(s.id), err)
		}

		// Ignore the sleep if we are receiving a handed off lease.
		if handoffLeaseID == "" {
			sleepWithContext(ctx, s.ReconnectDelay)
		}
	}
}

func (s *Store) acquireLeaseOrPrimaryInfo(ctx context.Context) (Lease, PrimaryInfo, error) {
	// Attempt to find an existing primary first.
	info, err := s.Leaser.PrimaryInfo(ctx)
	if err == ErrNoPrimary && !s.candidate {
		return nil, info, err // no primary, not eligible to become primary
	} else if err != nil && err != ErrNoPrimary {
		return nil, info, fmt.Errorf("fetch primary url: %w", err)
	} else if err == nil {
		return nil, info, nil
	}

	// If no primary, attempt to become primary.
	lease, err := s.Leaser.Acquire(ctx)
	if err == ErrPrimaryExists {
		// passthrough and retry primary info fetch
	} else if err != nil {
		return nil, info, fmt.Errorf("acquire lease: %w", err)
	} else if lease != nil {
		return lease, info, nil
	}

	// If we raced to become primary and another node beat us, retry the fetch.
	info, err = s.Leaser.PrimaryInfo(ctx)
	if err != nil {
		return nil, info, err
	}
	return nil, info, nil
}

// monitorLeaseAsPrimary monitors & renews the current lease.
// NOTE: This code is borrowed from the consul/api's RenewPeriodic() implementation.
func (s *Store) monitorLeaseAsPrimary(ctx context.Context, lease Lease) error {
	const timeout = 1 * time.Second

	// Attempt to destroy lease when we exit this function.
	var demoted bool
	closeLeaseOnExit := true
	defer func() {
		if closeLeaseOnExit {
			log.Printf("%s: exiting primary, destroying lease", FormatNodeID(s.id))
			if err := lease.Close(); err != nil {
				log.Printf("%s: cannot remove lease: %s", FormatNodeID(s.id), err)
			}
		} else {
			log.Printf("%s: exiting primary, preserving lease for handoff", FormatNodeID(s.id))
		}

		// Pause momentarily if this was a manual demotion.
		if demoted {
			log.Printf("%s: waiting for %s after demotion", FormatNodeID(s.id), s.DemoteDelay)
			sleepWithContext(ctx, s.DemoteDelay)
		}
	}()

	// If the leaser doesn't have a cluster ID yet, generate one or set it to ours.
	if v, err := s.Leaser.ClusterID(ctx); err != nil {
		return fmt.Errorf("set cluster id: %w", err)
	} else if v == "" {
		// Use existing ID or generate a new one.
		clusterID := s.ClusterID()
		if clusterID == "" {
			clusterID = GenerateClusterID()
		}

		// Update the cluster ID on the leaser.
		if err := s.Leaser.SetClusterID(ctx, clusterID); err != nil {
			return fmt.Errorf("set leaser cluster id: %w", err)
		}

		// Save the cluster ID to disk, in case we generated a new one above.
		if err := s.setClusterID(clusterID); err != nil {
			return fmt.Errorf("set local cluster id: %w", err)
		}

		log.Printf("set cluster id on %q lease %q", s.Leaser.Type(), clusterID)
	}

	// Mark as the primary node while we're in this function.
	s.mu.Lock()
	s.setLease(lease)
	primaryCtx := s.primaryCtx(context.Background())
	demoteCh := s.demoteCh
	s.mu.Unlock()

	// Mark store as ready if we've obtained primary status.
	s.markReady()

	// Run background goroutine to push data to long-term storage while we are primary.
	// This context is canceled when the lease is cleared on exit of the function.
	var g sync.WaitGroup
	defer g.Wait()

	if s.BackupClient != nil && s.BackupDelay > 0 {
		g.Add(1)
		go func() { defer g.Done(); s.monitorPrimaryBackup(primaryCtx) }()
	}

	// Ensure that we are no longer marked as primary once we exit this function.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.setLease(nil)
	}()

	// Notify host environment that we are primary.
	s.Environment.SetPrimaryStatus(ctx, true)
	defer func() { s.Environment.SetPrimaryStatus(ctx, false) }()

	waitDur := lease.TTL() / 2

	for {
		select {
		case <-time.After(waitDur):
			// Attempt to renew the lease. If the lease is gone then we need to
			// just exit and we can start over or connect to the new primary.
			//
			// If we just have a connection error then we'll try to more
			// aggressively retry the renewal until we exceed TTL.
			if err := lease.Renew(ctx); err == ErrLeaseExpired {
				return err
			} else if err != nil {
				// If our next renewal will exceed TTL, exit now.
				if time.Since(lease.RenewedAt())+timeout > lease.TTL() {
					time.Sleep(timeout)
					return ErrLeaseExpired
				}

				// Otherwise log error and try again after a shorter period.
				log.Printf("%s: lease renewal error, retrying: %s", FormatNodeID(s.id), err)
				waitDur = time.Second
				continue
			}

			// Renewal was successful, restart with low frequency.
			waitDur = lease.TTL() / 2

		case <-demoteCh:
			demoted = true
			log.Printf("%s: node manually demoted", FormatNodeID(s.id))
			return nil

		case nodeID := <-lease.HandoffCh():
			if err := s.processHandoff(ctx, nodeID, lease); err != nil {
				log.Printf("%s: handoff unsuccessful, continuing as primary", FormatNodeID(s.id))
				continue
			}
			closeLeaseOnExit = false
			return nil

		case <-ctx.Done():
			return nil // release lease when we shut down
		}
	}
}

// monitorPrimaryBackup executes in the background while the node is primary.
// The context is canceled when the primary status is lost.
func (s *Store) monitorPrimaryBackup(ctx context.Context) {
	log.Printf("begin primary backup stream: url=%s", s.BackupClient.URL())
	defer log.Printf("primary backup stream exiting")

	ticker := time.NewTicker(1 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.streamBackup(ctx, false); err != nil {
				log.Printf("backup stream failed, retrying: %s", err)
			}
		}
	}
}

// SyncBackup connects to a backup server performs a one-time sync.
func (s *Store) SyncBackup(ctx context.Context) error {
	return s.streamBackup(ctx, true)
}

// streamBackup connects to a backup server and continuously streams LTX files.
func (s *Store) streamBackup(ctx context.Context, oneTime bool) (err error) {
	// Start subscription immediately so we can collect any changes.
	subscription := s.SubscribeChangeSet(0)
	defer func() { _ = subscription.Close() }()

	slog.Info("begin streaming backup", slog.Duration("full-sync-interval", s.BackupFullSyncInterval))
	defer func() { slog.Info("exiting streaming backup") }()

	timer := time.NewTimer(0)
	defer timer.Stop()

	var posMapTickerCh <-chan time.Time
	if s.BackupFullSyncInterval > 0 {
		ticker := time.NewTicker(s.BackupFullSyncInterval)
		defer ticker.Stop()
		posMapTickerCh = ticker.C
	}

	var posMap map[string]ltx.Pos
	dirtySet := make(map[string]struct{})

LOOP:
	for {
		// If we don't have a position map yet or if it's been reset then fetch
		// a new one from the backup server and update our dirty set.
		if posMap == nil {
			slog.Debug("fetching position map from backup server")
			if posMap, err = s.BackupClient.PosMap(ctx); err != nil {
				return fmt.Errorf("fetch position map: %w", err)
			}
			for name := range posMap {
				dirtySet[name] = struct{}{}
			}
			for _, db := range s.DBs() {
				dirtySet[db.Name()] = struct{}{}
			}
		}

		slog.Debug("syncing databases to backup", slog.Int("dirty", len(dirtySet)))

		// Send pending transactions for each database.
		for name := range dirtySet {
			// Send all outstanding LTX files to the backup service. The backup
			// service is the data authority so if we cannot stream a contiguous
			// set of changes (e.g. position mismatch) then we need to revert to
			// the current snapshot state of the backup service.
			var pmErr *ltx.PosMismatchError
			newPos, err := s.streamBackupDB(ctx, name, posMap[name])
			if errors.As(err, &pmErr) {
				newPos, err = s.restoreDBFromBackup(ctx, name)
				if err != nil {
					return fmt.Errorf("restore from backup error (%q): %s", name, err)
				}
			} else if err != nil {
				return fmt.Errorf("backup stream error (%q): %s", name, err)
			}

			// If the position returned is empty then clear it from our map.
			if newPos.IsZero() {
				slog.Debug("no position, removing from backup sync", slog.String("name", name))
				delete(posMap, name)
				continue
			}

			// Update the latest position on the backup service for this database.
			slog.Debug("database synced to backup",
				slog.String("name", name),
				slog.String("pos", newPos.String()))
			posMap[name] = newPos
		}

		// If not continuous, exit here. Used for deterministic testing through SyncBackup().
		if oneTime {
			return nil
		}

		// Wait for new changes.
		select {
		case <-ctx.Done():
			return nil
		case <-posMapTickerCh: // periodically re-fetch position map from backup server
			posMap = nil
			continue LOOP
		case <-subscription.NotifyCh():
		}

		// Wait for a a delay to batch changes together.
		timer.Reset(s.BackupDelay)
		select {
		case <-ctx.Done():
			return nil
		case <-timer.C:
			dirtySet = subscription.DirtySet()
		}
	}
}

func (s *Store) streamBackupDB(ctx context.Context, name string, remotePos ltx.Pos) (newPos ltx.Pos, err error) {
	slog.Debug("sync database to backup", slog.String("name", name))

	db := s.DB(name)
	if db == nil {
		// TODO: Handle database deletion
		slog.Warn("restoring from backup", slog.String("name", name), slog.String("reason", "no-local"))
		return ltx.Pos{}, ltx.NewPosMismatchError(remotePos)
	}

	// Check local replication position.
	// If we haven't written anything yet then try to send data.
	localPos := db.Pos()
	if localPos.IsZero() {
		return localPos, nil
	}

	// If the database doesn't exist remotely, perform a full snapshot.
	if remotePos.IsZero() {
		return s.streamBackupDBSnapshot(ctx, db)
	}

	// If the position from the backup server is ahead of the primary then we
	// need to perform a recovery so that we snapshot from the backup server.
	if remotePos.TXID > localPos.TXID {
		slog.Warn("restoring from backup",
			slog.String("name", name),
			slog.Group("pos",
				slog.String("local", localPos.String()),
				slog.String("remote", remotePos.String()),
			),
			slog.String("reason", "remote-ahead"),
		)
		log.Printf("backup of database %q is ahead of local copy, restoring from backup", name)
		return ltx.Pos{}, ltx.NewPosMismatchError(remotePos) // backup TXID ahead of primary, needs recovery
	}

	// If the TXID matches the backup server, we need to ensure the checksum
	// does as well. If it doesn't, we need to grab a snapshot from the backup
	// server. If it does, then we can exit as we're already in sync.
	if remotePos.TXID == localPos.TXID {
		if remotePos.PostApplyChecksum != localPos.PostApplyChecksum {
			slog.Warn("restoring from backup",
				slog.String("name", name),
				slog.Group("pos",
					slog.String("local", localPos.String()),
					slog.String("remote", remotePos.String()),
				),
				slog.String("reason", "chksum-mismatch"),
			)
			return ltx.Pos{}, ltx.NewPosMismatchError(remotePos) // same TXID, different checksum
		}

		slog.Debug("database in sync with backup, skipping", slog.String("name", name))
		return localPos, nil // already in sync
	}

	assert(remotePos.TXID < localPos.TXID, "remote/local position must be ordered")

	// OPTIMIZE: Check that remote postApplyChecksum equals next TXID's preApplyChecksum

	// Collect all transaction files to catch up from remote position to current position.
	var rdrs []io.Reader
	defer func() {
		for _, r := range rdrs {
			_ = r.(io.Closer).Close()
		}
	}()

	for txID, n := remotePos.TXID+1, 0; txID <= localPos.TXID && n < MaxBackupLTXFileN; txID, n = txID+1, n+1 {
		f, err := db.OpenLTXFile(txID)
		if os.IsNotExist(err) {
			slog.Warn("restoring from backup",
				slog.String("name", name),
				slog.Group("pos",
					slog.String("local", localPos.String()),
					slog.String("remote", remotePos.String()),
				),
				slog.String("txid", txID.String()),
				slog.String("reason", "ltx-not-found"),
			)
			return ltx.Pos{}, ltx.NewPosMismatchError(remotePos)
		} else if err != nil {
			return ltx.Pos{}, fmt.Errorf("open ltx file: %w", err)
		}
		rdrs = append(rdrs, f)
	}

	// Compact LTX files through a pipe so we can pass it to the backup client.
	pr, pw := io.Pipe()
	var pos ltx.Pos
	go func() {
		compactor := ltx.NewCompactor(pw, rdrs)
		compactor.HeaderFlags = s.ltxHeaderFlags()
		if err := compactor.Compact(ctx); err != nil {
			_ = pw.CloseWithError(err)
			return
		}

		pos = ltx.NewPos(compactor.Header().MaxTXID, compactor.Trailer().PostApplyChecksum)
		_ = pw.Close()
	}()

	var pmErr *ltx.PosMismatchError
	hwm, err := s.BackupClient.WriteTx(ctx, name, pr)
	if errors.As(err, &pmErr) {
		slog.Warn("restoring from backup",
			slog.String("name", name),
			slog.Group("pos",
				slog.String("local", localPos.String()),
				slog.String("remote", pmErr.Pos.String()),
			),
			slog.String("reason", "out-of-sync"),
		)
		return ltx.Pos{}, pmErr
	} else if err != nil {
		return ltx.Pos{}, fmt.Errorf("write backup tx: %w", err)
	}
	db.SetHWM(hwm)

	return pos, nil
}

// streamBackupDBSnapshot writes the entire snapshot to the backup client.
// This is done when no data exists on the remote backup for the database.
func (s *Store) streamBackupDBSnapshot(ctx context.Context, db *DB) (newPos ltx.Pos, err error) {
	var v atomic.Value
	v.Store(ltx.Pos{})

	// Run snapshot through a goroutine so we can pipe it to the backup writer.
	pr, pw := io.Pipe()
	go func() {
		header, trailer, err := db.WriteSnapshotTo(ctx, pw)
		v.Store(ltx.NewPos(header.MaxTXID, trailer.PostApplyChecksum))
		_ = pw.CloseWithError(err)
	}()

	hwm, err := s.BackupClient.WriteTx(ctx, db.Name(), pr)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("write backup tx snapshot: %w", err)
	}
	db.SetHWM(hwm)

	pos := v.Load().(ltx.Pos)
	return pos, nil
}

// restoreDBFromBackup pulls the current snapshot from the backup service and
// restores it to the local database. The backup service acts as the data
// authority so this occurs when we cannot provide a contiguous series of
// transaction files to the backup service.
func (s *Store) restoreDBFromBackup(ctx context.Context, name string) (newPos ltx.Pos, err error) {
	// Read the snapshot from the backup service.
	rc, err := s.BackupClient.FetchSnapshot(ctx, name)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("fetch backup snapshot: %w", err)
	}
	defer func() { _ = rc.Close() }()

	// Create the database if it doesn't exist.
	db, err := s.CreateDBIfNotExists(name)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("create database: %s", err)
	}

	t := time.Now()
	slog.Debug("beginning database restore from backup",
		slog.String("name", name),
		slog.String("prev_pos", db.Pos().String()),
	)

	// Acquire the write lock so we can apply the snapshot.
	guard, err := db.AcquireWriteLock(ctx, nil)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("acquire write lock: %s", err)
	}
	defer guard.Unlock()

	if err := db.recover(ctx); err != nil {
		return ltx.Pos{}, fmt.Errorf("recover: %s", err)
	}

	// Wrap request body in a chunked reader.
	ltxPath, err := db.WriteLTXFileAt(ctx, rc)
	if err != nil {
		return ltx.Pos{}, fmt.Errorf("write ltx file: %s", err)
	}

	// Apply transaction to database.
	if err := db.ApplyLTXNoLock(ltxPath, true); err != nil {
		return ltx.Pos{}, fmt.Errorf("cannot apply ltx: %s", err)
	}
	newPos = db.Pos()

	slog.Warn("database restore complete",
		slog.String("name", name),
		slog.String("pos", newPos.String()),
		slog.Duration("elapsed", time.Since(t)),
	)

	return newPos, nil
}

func (s *Store) processHandoff(ctx context.Context, nodeID uint64, lease Lease) error {
	// Find subscriber to ensure it is still connected.
	sub := s.SubscriberByNodeID(nodeID)
	if sub == nil {
		return fmt.Errorf("node is no longer connected")
	}

	// Renew the lease one last time before handing off.
	if err := lease.Renew(ctx); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeoutCause(ctx, 5*time.Second, fmt.Errorf("handoff processing timeout"))
	defer cancel()

	select {
	case <-ctx.Done():
		return context.Cause(ctx)
	case sub.HandoffCh() <- lease.ID():
		return nil
	}
}

// monitorLeaseAsReplica tries to connect to the primary node and stream down changes.
func (s *Store) monitorLeaseAsReplica(ctx context.Context, info PrimaryInfo) (handoffLeaseID string, err error) {
	if s.Client == nil {
		return "", fmt.Errorf("no client set, skipping replica monitor")
	}

	// Store the URL of the primary while we're in this function.
	s.mu.Lock()
	s.setPrimaryInfo(&info)
	s.mu.Unlock()

	// Clear the primary URL once we leave this function since we can no longer connect.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.setPrimaryInfo(nil)
	}()

	posMap := s.PosMap()
	st, err := s.Client.Stream(ctx, info.AdvertiseURL, s.id, posMap, s.DatabaseFilter)
	if err != nil {
		return "", fmt.Errorf("connect to primary: %s ('%s')", err, info.AdvertiseURL)
	}
	defer func() { _ = st.Close() }()

	// Adopt cluster ID from primary node if we don't have a cluster ID yet.
	if s.ClusterID() == "" && st.ClusterID() != "" {
		if err := s.setClusterID(st.ClusterID()); err != nil {
			return "", fmt.Errorf("set local cluster id: %w", err)
		}
	}

	// Verify that we are attaching onto a node in the same cluster.
	if s.ClusterID() != st.ClusterID() {
		return "", fmt.Errorf("cannot stream from primary with a different cluster id: %s <> %s", s.ClusterID(), st.ClusterID())
	}

	for {
		frame, err := ReadStreamFrame(st)
		if err == io.EOF {
			return "", nil // clean disconnect
		} else if err != nil {
			return "", fmt.Errorf("next frame: %w", err)
		}

		switch frame := frame.(type) {
		case *LTXStreamFrame:
			if err := s.processLTXStreamFrame(ctx, frame, chunk.NewReader(st)); err != nil {
				return "", fmt.Errorf("process ltx stream frame: %w", err)
			}
		case *ReadyStreamFrame:
			// Mark store as ready once we've received an initial replication set.
			s.markReady()
		case *EndStreamFrame:
			// Server cleanly disconnected
			return "", nil
		case *DropDBStreamFrame:
			log.Printf("deprecated drop db frame received, skipping")
		case *HandoffStreamFrame:
			return frame.LeaseID, nil
		case *HWMStreamFrame:
			if db := s.DB(frame.Name); db != nil {
				db.SetHWM(frame.TXID)
			}
		case *HeartbeatStreamFrame:
			s.setPrimaryTimestamp(frame.Timestamp)
		default:
			return "", fmt.Errorf("invalid stream frame type: 0x%02x", frame.Type())
		}
	}
}

// monitorRetention periodically enforces retention of LTX files on the databases.
func (s *Store) monitorRetention(ctx context.Context) error {
	ticker := time.NewTicker(s.RetentionMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.EnforceRetention(ctx); err != nil {
				return err
			}
		}
	}
}

// monitorHaltLock periodically check all halt locks for expiration.
func (s *Store) monitorHaltLock(ctx context.Context) error {
	ticker := time.NewTicker(s.HaltLockMonitorInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			s.EnforceHaltLockExpiration(ctx)
		}
	}
}

// EnforceHaltLockExpiration expires any overdue HALT locks.
func (s *Store) EnforceHaltLockExpiration(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, db := range s.dbs {
		db.EnforceHaltLockExpiration(ctx)
	}
}

// Recover forces a rollback (journal) or checkpoint (wal) on all open databases.
// This is done when switching the primary/replica state.
func (s *Store) Recover(ctx context.Context) (err error) {
	for _, db := range s.DBs() {
		if err := db.Recover(ctx); err != nil {
			return fmt.Errorf("db %q: %w", db.Name(), err)
		}
	}
	return nil
}

// EnforceRetention enforces retention of LTX files on all databases.
func (s *Store) EnforceRetention(ctx context.Context) (err error) {
	// Skip enforcement if not set.
	if s.Retention <= 0 {
		return nil
	}

	minTime := time.Now().Add(-s.Retention).UTC()

	for _, db := range s.DBs() {
		if e := db.EnforceRetention(ctx, minTime); err == nil {
			err = fmt.Errorf("cannot enforce retention on db %q: %w", db.Name(), e)
		}
	}
	return nil
}

func (s *Store) processLTXStreamFrame(ctx context.Context, frame *LTXStreamFrame, src io.Reader) (err error) {
	db, err := s.CreateDBIfNotExists(frame.Name)
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	hdr, data, err := ltx.DecodeHeader(src)
	if err != nil {
		return fmt.Errorf("peek ltx header: %w", err)
	}
	src = io.MultiReader(bytes.NewReader(data), src)

	TraceLog.Printf("[ProcessLTXStreamFrame.Begin(%s)]: txid=%s-%s, preApplyChecksum=%s", db.Name(), hdr.MinTXID.String(), hdr.MaxTXID.String(), hdr.PreApplyChecksum)
	defer func() {
		TraceLog.Printf("[ProcessLTXStreamFrame.End(%s)]: %s", db.name, errorKeyValue(err))
	}()

	// Acquire lock unless we are waiting for a database position, in which case,
	// we already have the lock.
	guardSet, err := db.AcquireWriteLock(ctx, nil)
	if err != nil {
		return err
	}
	defer guardSet.Unlock()

	// Skip frame if it already occurred on this node. This can happen if the
	// replica node created the transaction and forwarded it to the primary.
	if hdr.NodeID == s.ID() {
		dec := ltx.NewDecoder(src)
		if err := dec.Verify(); err != nil {
			return fmt.Errorf("verify duplicate ltx file: %w", err)
		}
		if _, err := io.Copy(io.Discard, src); err != nil {
			return fmt.Errorf("discard ltx body: %w", err)
		}
		return nil
	}

	// If we receive an LTX file while holding the remote HALT lock then the
	// remote lock must have expired or been released so we can clear it locally.
	//
	// We also hold the local WRITE lock so a local write cannot be in-progress.
	if haltLock := db.RemoteHaltLock(); haltLock != nil {
		TraceLog.Printf("[ProcessLTXStreamFrame.Unhalt(%s)]: replica holds HALT lock but received LTX file, unsetting HALT lock", db.Name())
		if err := db.UnsetRemoteHaltLock(ctx, haltLock.ID); err != nil {
			return fmt.Errorf("release remote halt lock: %w", err)
		}
	}

	// Verify LTX file pre-apply checksum matches the current database position
	// unless this is a snapshot, which will overwrite all data.
	if !hdr.IsSnapshot() {
		expectedPos := ltx.Pos{
			TXID:              hdr.MinTXID - 1,
			PostApplyChecksum: hdr.PreApplyChecksum,
		}
		if pos := db.Pos(); pos != expectedPos {
			return fmt.Errorf("position mismatch on db %q: %s <> %s", db.Name(), pos, expectedPos)
		}
	}

	// Write LTX file to a temporary file and we'll atomically rename later.
	path := db.LTXPath(hdr.MinTXID, hdr.MaxTXID)
	tmpPath := fmt.Sprintf("%s.%d.tmp", path, rand.Int())
	defer func() { _ = s.OS.Remove("PROCESSLTX", tmpPath) }()

	f, err := s.OS.Create("PROCESSLTX", tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create temp ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()

	n, err := io.Copy(f, src)
	if err != nil {
		return fmt.Errorf("write ltx file: %w", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync ltx file: %w", err)
	}

	// Atomically rename file.
	if err := s.OS.Rename("PROCESSLTX", tmpPath, path); err != nil {
		return fmt.Errorf("rename ltx file: %w", err)
	} else if err := internal.Sync(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync ltx dir: %w", err)
	}

	// Update metrics
	dbLTXCountMetricVec.WithLabelValues(db.Name()).Inc()
	dbLTXBytesMetricVec.WithLabelValues(db.Name()).Set(float64(n))

	// Remove other LTX files after a snapshot.
	if hdr.IsSnapshot() {
		dir, file := filepath.Split(path)
		log.Printf("snapshot received for %q, removing other ltx files: %s", db.Name(), file)
		if err := removeFilesExcept(s.OS, dir, file); err != nil {
			return fmt.Errorf("remove ltx except snapshot: %w", err)
		}
	}

	// Attempt to apply the LTX file to the database.
	if err := db.ApplyLTXNoLock(path, true); err != nil {
		return fmt.Errorf("apply ltx: %w", err)
	}

	// Don't consider ltx timestamps for PrimaryTimestamp until initial
	// replication is finished. Users might assume databases are up-to-date
	// when they're not.
	if s.isReady() {
		s.setPrimaryTimestamp(hdr.Timestamp)
	}

	return nil
}

// ltxHeaderFlags returns flags used for the LTX header.
func (s *Store) ltxHeaderFlags() uint32 {
	var flags uint32
	if s.Compress {
		flags |= ltx.HeaderFlagCompressLZ4
	}
	return flags
}

// PrimaryTimestamp returns the last timestamp (ms since epoch) received from
// the primary. Returns -1 if we are the primary or if we haven't finished
// initial replication yet.
func (s *Store) PrimaryTimestamp() int64 {
	return s.primaryTimestamp.Load()
}

// 0 means we're primary. -1 means we're a new replica
func (s *Store) setPrimaryTimestamp(ts int64) {
	s.primaryTimestamp.Store(ts)

	if invalidator := s.Invalidator; invalidator != nil {
		if err := invalidator.InvalidateLag(); err != nil {
			log.Printf("error invalidating .lag cache: %s\n", err)
		}
	}
}

// Lag returns the number of seconds that the local instance is lagging
// behind the primary node. Returns 0 if the node is the primary or if the
// node is not marked as ready yet.
func (s *Store) Lag() time.Duration {
	switch ts := s.PrimaryTimestamp(); ts {
	case 0, -1:
		return 0 // primary or not ready
	default:
		return time.Duration(time.Now().UnixMilli()-ts) * time.Millisecond
	}
}

// Expvar returns a variable for debugging output.
func (s *Store) Expvar() expvar.Var { return (*StoreVar)(s) }

var _ expvar.Var = (*StoreVar)(nil)

type StoreVar Store

func (v *StoreVar) String() string {
	s := (*Store)(v)
	m := &storeVarJSON{
		IsPrimary: s.IsPrimary(),
		Candidate: s.candidate,
		DBs:       make(map[string]*dbVarJSON),
	}

	for _, db := range s.DBs() {
		pos := db.Pos()

		dbJSON := &dbVarJSON{
			Name:     db.Name(),
			TXID:     pos.TXID.String(),
			Checksum: pos.PostApplyChecksum.String(),
		}

		dbJSON.Locks.Pending = db.pendingLock.State().String()
		dbJSON.Locks.Shared = db.sharedLock.State().String()
		dbJSON.Locks.Reserved = db.reservedLock.State().String()

		dbJSON.Locks.Write = db.writeLock.State().String()
		dbJSON.Locks.Ckpt = db.ckptLock.State().String()
		dbJSON.Locks.Recover = db.recoverLock.State().String()
		dbJSON.Locks.Read0 = db.read0Lock.State().String()
		dbJSON.Locks.Read1 = db.read1Lock.State().String()
		dbJSON.Locks.Read2 = db.read2Lock.State().String()
		dbJSON.Locks.Read3 = db.read3Lock.State().String()
		dbJSON.Locks.Read4 = db.read4Lock.State().String()
		dbJSON.Locks.DMS = db.dmsLock.State().String()

		m.DBs[db.Name()] = dbJSON
	}

	b, err := json.Marshal(m)
	if err != nil {
		return "null"
	}
	return string(b)
}

type storeVarJSON struct {
	IsPrimary bool                  `json:"isPrimary"`
	Candidate bool                  `json:"candidate"`
	DBs       map[string]*dbVarJSON `json:"dbs"`
}

// ChangeSetSubscriber subscribes to changes to databases in the store.
//
// It implements a set of "dirty" databases instead of a channel of all events
// as clients can be slow and we don't want to cause channels to back up. It
// is the responsibility of the caller to determine the state changes which is
// usually just checking the position of the client versus the store's database.
type ChangeSetSubscriber struct {
	store  *Store
	nodeID uint64

	mu        sync.Mutex
	notifyCh  chan struct{}
	dirtySet  map[string]struct{}
	handoffCh chan string
}

// newChangeSetSubscriber returns a new instance of Subscriber associated with a store.
func newChangeSetSubscriber(store *Store, nodeID uint64) *ChangeSetSubscriber {
	s := &ChangeSetSubscriber{
		store:     store,
		nodeID:    nodeID,
		notifyCh:  make(chan struct{}, 1),
		dirtySet:  make(map[string]struct{}),
		handoffCh: make(chan string),
	}
	return s
}

// Close removes the subscriber from the store.
func (s *ChangeSetSubscriber) Close() error {
	s.store.UnsubscribeChangeSet(s)
	return nil
}

// NodeID returns the ID of the subscribed node.
func (s *ChangeSetSubscriber) NodeID() uint64 { return s.nodeID }

// NotifyCh returns a channel that receives a value when the dirty set has changed.
func (s *ChangeSetSubscriber) NotifyCh() <-chan struct{} { return s.notifyCh }

// HandoffCh returns a channel that returns a lease ID on handoff.
func (s *ChangeSetSubscriber) HandoffCh() chan string { return s.handoffCh }

// MarkDirty marks a database ID as dirty.
func (s *ChangeSetSubscriber) MarkDirty(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtySet[name] = struct{}{}

	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// DirtySet returns a set of database IDs that have changed since the last call
// to DirtySet(). This call clears the set.
func (s *ChangeSetSubscriber) DirtySet() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirtySet := s.dirtySet
	s.dirtySet = make(map[string]struct{})
	return dirtySet
}

const EventChannelBufferSize = 1024

// EventSubscriber subscribes to generic store events.
type EventSubscriber struct {
	store *Store
	ch    chan Event
}

// newEventSubscriber returns a new instance of Subscriber associated with a store.
func newEventSubscriber(store *Store) *EventSubscriber {
	return &EventSubscriber{
		store: store,
		ch:    make(chan Event, EventChannelBufferSize),
	}
}

// Stop closes the subscriber and removes it from the store.
func (s *EventSubscriber) Stop() {
	s.store.UnsubscribeEvents(s)
}

// C returns a channel that receives event notifications.
// If caller cannot read events fast enough then channel will be closed.
func (s *EventSubscriber) C() <-chan Event { return s.ch }

const (
	EventTypeInit          = "init"
	EventTypeTx            = "tx"
	EventTypePrimaryChange = "primaryChange"
)

// Event represents a generic event.
type Event struct {
	Type string `json:"type"`
	DB   string `json:"db,omitempty"`
	Data any    `json:"data,omitempty"`
}

func (e *Event) UnmarshalJSON(data []byte) error {
	var v eventJSON
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}

	e.Type = v.Type
	e.DB = v.DB

	switch v.Type {
	case EventTypeInit:
		e.Data = &InitEventData{}
	case EventTypeTx:
		e.Data = &TxEventData{}
	case EventTypePrimaryChange:
		e.Data = &PrimaryChangeEventData{}
	default:
		e.Data = nil
	}
	if err := json.Unmarshal(v.Data, &e.Data); err != nil {
		return err
	}
	return nil
}

type eventJSON struct {
	Type string          `json:"type"`
	DB   string          `json:"db,omitempty"`
	Data json.RawMessage `json:"data,omitempty"`
}

type InitEventData struct {
	IsPrimary bool   `json:"isPrimary"`
	Hostname  string `json:"hostname,omitempty"`
}

type TxEventData struct {
	TXID              ltx.TXID     `json:"txID"`
	PostApplyChecksum ltx.Checksum `json:"postApplyChecksum"`
	PageSize          uint32       `json:"pageSize"`
	Commit            uint32       `json:"commit"`
	Timestamp         time.Time    `json:"timestamp"`
}

type PrimaryChangeEventData struct {
	IsPrimary bool   `json:"isPrimary"`
	Hostname  string `json:"hostname,omitempty"`
}

var _ context.Context = (*primaryCtx)(nil)

// primaryCtx represents a context that is marked done when the node loses its primary status.
type primaryCtx struct {
	parent    context.Context
	primaryCh chan struct{}
	done      chan struct{}
}

func newPrimaryCtx(parent context.Context, primaryCh chan struct{}) *primaryCtx {
	ctx := &primaryCtx{
		parent:    parent,
		primaryCh: primaryCh,
		done:      make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.primaryCh:
			close(ctx.done)
		case <-ctx.parent.Done():
			close(ctx.done)
		}
	}()

	return ctx
}

func (ctx *primaryCtx) Deadline() (deadline time.Time, ok bool) {
	return ctx.parent.Deadline()
}

func (ctx *primaryCtx) Done() <-chan struct{} {
	return ctx.done
}

func (ctx *primaryCtx) Err() error {
	select {
	case <-ctx.primaryCh:
		return ErrLeaseExpired
	default:
		return ctx.parent.Err()
	}
}

func (ctx *primaryCtx) Value(key any) any {
	return ctx.parent.Value(key)
}

// removeFilesExcept removes all files from a directory except a given filename.
// Attempts to remove all files, even in the event of an error. Returns the
// first error encountered.
func removeFilesExcept(osys OS, dir, filename string) (retErr error) {
	ents, err := osys.ReadDir("REMOVEFILESEXCEPT", dir)
	if err != nil {
		return err
	}

	for _, ent := range ents {
		// Skip directories & exception file.
		if ent.IsDir() || ent.Name() == filename {
			continue
		}
		if err := osys.Remove("REMOVEFILESEXCEPT", filepath.Join(dir, ent.Name())); retErr == nil {
			retErr = err
		}
	}

	return retErr
}

// sleepWithContext sleeps for a given amount of time or until the context is canceled.
func sleepWithContext(ctx context.Context, d time.Duration) {
	// Skip timer creation if context is already canceled.
	if ctx.Err() != nil {
		return
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
	case <-timer.C:
	}
}

// Store metrics.
var (
	storeDBCountMetric = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "litefs_db_count",
		Help: "Number of managed databases.",
	})

	storeIsPrimaryMetric = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "litefs_is_primary",
		Help: "Primary status of the node.",
	})

	storeSubscriberCountMetric = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "litefs_subscriber_count",
		Help: "Number of connected subscribers",
	})

	_ = promauto.NewGaugeFunc(prometheus.GaugeOpts{
		Name: "litefs_lag_seconds",
		Help: "Lag behind the primary node, in seconds",
	}, func() float64 {
		if s := GlobalStore.Load(); s != nil {
			return s.(*Store).Lag().Seconds()
		}
		return 0
	})
)
