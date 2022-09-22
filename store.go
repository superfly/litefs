package litefs

import (
	"bytes"
	"context"
	crand "crypto/rand"
	"encoding/json"
	"expvar"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// IDLength is the length of a node ID, in bytes.
const IDLength = 24

// Default store settings.
const (
	DefaultRetentionDuration        = 1 * time.Minute
	DefaultRetentionMonitorInterval = 1 * time.Minute
)

// Store represents a collection of databases.
type Store struct {
	mu   sync.Mutex
	path string

	id          string // unique node id
	dbs         map[string]*DB
	subscribers map[*Subscriber]struct{}

	isPrimary   bool          // if true, store is current primary
	primaryCh   chan struct{} // closed when primary loses leadership
	primaryInfo *PrimaryInfo  // contains info about the current primary
	candidate   bool          // if true, we are eligible to become the primary
	readyCh     chan struct{} // closed when primary found or acquired

	ctx    context.Context
	cancel func()
	g      errgroup.Group

	// Client used to connect to other LiteFS instances.
	Client Client

	// Leaser manages the lease that controls leader election.
	Leaser Leaser

	// Length of time to retain LTX files.
	RetentionDuration        time.Duration
	RetentionMonitorInterval time.Duration

	// Callback to notify kernel of file changes.
	Invalidator Invalidator

	// If true, computes and verifies the checksum of the entire database
	// after every transaction. Should only be used during testing.
	StrictVerify bool
}

// NewStore returns a new instance of Store.
func NewStore(path string, candidate bool) *Store {
	primaryCh := make(chan struct{})
	close(primaryCh)

	s := &Store{
		path: path,

		dbs: make(map[string]*DB),

		subscribers: make(map[*Subscriber]struct{}),
		candidate:   candidate,
		primaryCh:   primaryCh,
		readyCh:     make(chan struct{}),

		RetentionDuration:        DefaultRetentionDuration,
		RetentionMonitorInterval: DefaultRetentionMonitorInterval,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

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

// ID returns the unique identifier for this instance. Available after Open().
// Persistent across restarts if underlying storage is persistent.
func (s *Store) ID() string {
	return s.id
}

// Open initializes the store based on files in the data directory.
func (s *Store) Open() error {
	if s.Leaser == nil {
		return fmt.Errorf("leaser required")
	}

	if err := os.MkdirAll(s.path, 0777); err != nil {
		return err
	}

	if err := s.initID(); err != nil {
		return fmt.Errorf("init node id: %w", err)
	}

	if err := s.openDatabases(); err != nil {
		return fmt.Errorf("open databases: %w", err)
	}

	// Begin background replication monitor.
	s.g.Go(func() error { return s.monitorLease(s.ctx) })

	// Begin retention monitor.
	if s.RetentionMonitorInterval > 0 {
		s.g.Go(func() error { return s.monitorRetention(s.ctx) })
	}

	return nil
}

// initID initializes an identifier that is unique to this node.
func (s *Store) initID() error {
	filename := filepath.Join(s.path, "id")

	// Read existing ID from file, if it exists.
	if buf, err := os.ReadFile(filename); err != nil && !os.IsNotExist(err) {
		return err
	} else if err == nil {
		s.id = string(bytes.TrimSpace(buf))
		return nil // existing ID
	}

	// Generate a new node ID if file doesn't exist.
	b := make([]byte, IDLength/2)
	if _, err := io.ReadFull(crand.Reader, b); err != nil {
		return fmt.Errorf("generate id: %w", err)
	}
	id := fmt.Sprintf("%X", b)

	f, err := os.Create(filename)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	if _, err := f.Write([]byte(id + "\n")); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	s.id = id

	return nil
}

func (s *Store) openDatabases() error {
	if err := os.MkdirAll(s.DBDir(), 0777); err != nil {
		return err
	}

	f, err := os.Open(s.DBDir())
	if err != nil {
		return fmt.Errorf("open databases dir: %w", err)
	}
	defer func() { _ = f.Close() }()

	fis, err := f.Readdir(-1)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}
	for _, fi := range fis {
		if err := s.openDatabase(fi.Name()); err != nil {
			return fmt.Errorf("open database(%q): %w", fi.Name(), err)
		}
	}

	if err := f.Close(); err != nil {
		return fmt.Errorf("close databases dir: %w", err)
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
func (s *Store) Close() error {
	s.cancel()
	return s.g.Wait()
}

// ReadyCh returns a channel that is closed once the store has become primary
// or once it has connected to the primary.
func (s *Store) ReadyCh() chan struct{} {
	return s.readyCh
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

// IsPrimary returns true if store has a lease to be the primary.
func (s *Store) IsPrimary() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isPrimary
}

func (s *Store) setIsPrimary(v bool) {
	// Create a new channel to notify about primary loss when becoming primary.
	// Or close existing channel if we are losing our primary status.
	if s.isPrimary != v {
		if v {
			s.primaryCh = make(chan struct{})
		} else {
			close(s.primaryCh)
		}
	}

	// Update state.
	s.isPrimary = v

	// Update metrics.
	if s.isPrimary {
		storeIsPrimaryMetric.Set(1)
	} else {
		storeIsPrimaryMetric.Set(0)
	}
}

// PrimaryCtx wraps ctx with another context that will cancel when no longer primary.
func (s *Store) PrimaryCtx(ctx context.Context) context.Context {
	s.mu.Lock()
	defer s.mu.Unlock()
	return newPrimaryCtx(ctx, s.primaryCh)
}

// PrimaryInfo returns info about the current primary.
func (s *Store) PrimaryInfo() *PrimaryInfo {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.primaryInfo.Clone()
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
func (s *Store) CreateDB(name string) (*DB, *os.File, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Verify database doesn't already exist.
	if _, ok := s.dbs[name]; ok {
		return nil, nil, ErrDatabaseExists
	}

	// Generate database directory with name file & empty database file.
	dbPath := s.DBPath(name)
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(filepath.Join(dbPath, "database"), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
	if err != nil {
		return nil, nil, err
	}

	// Create new database instance and add to maps.
	db := NewDB(s, name, dbPath)
	if err := db.Open(); err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	s.dbs[name] = db

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
	if err := os.MkdirAll(dbPath, 0777); err != nil {
		return nil, err
	}

	if err := os.WriteFile(filepath.Join(dbPath, "database"), nil, 0666); err != nil {
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
func (s *Store) PosMap() map[string]Pos {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := make(map[string]Pos, len(s.dbs))
	for _, db := range s.dbs {
		m[db.Name()] = db.Pos()
	}
	return m
}

// Subscribe creates a new subscriber for store changes.
func (s *Store) Subscribe() *Subscriber {
	s.mu.Lock()
	defer s.mu.Unlock()

	sub := newSubscriber(s)
	s.subscribers[sub] = struct{}{}

	storeSubscriberCountMetric.Set(float64(len(s.subscribers)))
	return sub
}

// Unsubscribe removes a subscriber from the store.
func (s *Store) Unsubscribe(sub *Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()

	delete(s.subscribers, sub)
	storeSubscriberCountMetric.Set(float64(len(s.subscribers)))
}

// MarkDirty marks a database dirty on all subscribers.
func (s *Store) MarkDirty(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markDirty(name)
}

func (s *Store) markDirty(name string) {
	for sub := range s.subscribers {
		sub.MarkDirty(name)
	}
}

// monitorLease continuously handles either the leader lease or replicates from the primary.
func (s *Store) monitorLease(ctx context.Context) error {
	for {
		// Exit if store is closed.
		if err := ctx.Err(); err != nil {
			return nil
		}

		// Attempt to either obtain a primary lock or read the current primary.
		lease, info, err := s.acquireLeaseOrPrimaryInfo(ctx)
		if err == ErrNoPrimary && !s.candidate {
			log.Printf("cannot find primary & ineligible to become primary, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		} else if err != nil {
			log.Printf("cannot acquire lease or find primary, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Monitor as primary if we have obtained a lease.
		if lease != nil {
			log.Printf("primary lease acquired, advertising as %s", s.Leaser.AdvertiseURL())
			if err := s.monitorLeaseAsPrimary(ctx, lease); err != nil {
				log.Printf("primary lease lost, retrying: %s", err)
			}
			continue
		}

		// Monitor as replica if another primary already exists.
		log.Printf("existing primary found (%s), connecting as replica", info.Hostname)
		if err := s.monitorLeaseAsReplica(ctx, info); err != nil {
			log.Printf("replica disconnected, retrying: %s", err)
		}
		time.Sleep(1 * time.Second)
	}
}

func (s *Store) acquireLeaseOrPrimaryInfo(ctx context.Context) (Lease, *PrimaryInfo, error) {
	// Attempt to find an existing primary first.
	info, err := s.Leaser.PrimaryInfo(ctx)
	if err == ErrNoPrimary && !s.candidate {
		return nil, nil, err // no primary, not eligible to become primary
	} else if err != nil && err != ErrNoPrimary {
		return nil, nil, fmt.Errorf("fetch primary url: %w", err)
	} else if err == nil {
		return nil, &info, nil
	}

	// If no primary, attempt to become primary.
	lease, err := s.Leaser.Acquire(ctx)
	if err != nil && err != ErrPrimaryExists {
		return nil, nil, fmt.Errorf("acquire lease: %w", err)
	} else if lease != nil {
		return lease, nil, nil
	}

	// If we raced to become primary and another node beat us, retry the fetch.
	info, err = s.Leaser.PrimaryInfo(ctx)
	if err != nil {
		return nil, nil, err
	}
	return nil, &info, nil
}

// monitorLeaseAsPrimary monitors & renews the current lease.
// NOTE: This code is borrowed from the consul/api's RenewPeriodic() implementation.
func (s *Store) monitorLeaseAsPrimary(ctx context.Context, lease Lease) error {
	const timeout = 1 * time.Second

	// Attempt to destroy lease when we exit this function.
	defer func() {
		log.Printf("exiting primary, destroying lease")
		if err := lease.Close(); err != nil {
			log.Printf("cannot remove lease: %s", err)
		}
	}()

	// Mark as the primary node while we're in this function.
	s.mu.Lock()
	s.setIsPrimary(true)
	s.mu.Unlock()

	// Mark store as ready if we've obtained primary status.
	s.markReady()

	// Ensure that we are no longer marked as primary once we exit this function.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.setIsPrimary(false)
	}()

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
				log.Printf("lease renewal error, retrying: %s", err)
				waitDur = time.Second
				continue
			}

			// Renewal was successful, restart with low frequency.
			waitDur = lease.TTL() / 2

		case <-ctx.Done():
			return nil // release lease when we shut down
		}
	}
}

// monitorLeaseAsReplica tries to connect to the primary node and stream down changes.
func (s *Store) monitorLeaseAsReplica(ctx context.Context, info *PrimaryInfo) error {
	if s.Client == nil {
		return fmt.Errorf("no client set, skipping replica monitor")
	}

	// Store the URL of the primary while we're in this function.
	s.mu.Lock()
	s.primaryInfo = info
	s.mu.Unlock()

	// Clear the primary URL once we leave this function since we can no longer connect.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.primaryInfo = nil
	}()

	posMap := s.PosMap()
	st, err := s.Client.Stream(ctx, info.AdvertiseURL, s.id, posMap)
	if err != nil {
		return fmt.Errorf("connect to primary: %s ('%s')", err, info.AdvertiseURL)
	}

	for {
		frame, err := ReadStreamFrame(st)
		if err == io.EOF {
			return nil // clean disconnect
		} else if err != nil {
			return fmt.Errorf("next frame: %w", err)
		}

		switch frame := frame.(type) {
		case *LTXStreamFrame:
			if err := s.processLTXStreamFrame(ctx, frame, st); err != nil {
				return fmt.Errorf("process ltx stream frame: %w", err)
			}
		case *ReadyStreamFrame:
			// Mark store as ready once we've received an initial replication set.
			log.Printf("recv frame<ready>")
			s.markReady()
		default:
			return fmt.Errorf("invalid stream frame type: 0x%02x", frame.Type())
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

// EnforceRetention enforces retention of LTX files on all databases.
func (s *Store) EnforceRetention(ctx context.Context) (err error) {
	minTime := time.Now().Add(-s.RetentionDuration).UTC()

	for _, db := range s.DBs() {
		if e := db.EnforceRetention(ctx, minTime); err == nil {
			err = fmt.Errorf("cannot enforce retention on db %q: %w", db.Name(), e)
		}
	}
	return nil
}

func (s *Store) processLTXStreamFrame(ctx context.Context, frame *LTXStreamFrame, src io.Reader) error {
	db, err := s.CreateDBIfNotExists(frame.Name)
	if err != nil {
		return fmt.Errorf("create database: %w", err)
	}

	r := ltx.NewReader(src)
	if err := r.PeekHeader(); err != nil {
		return fmt.Errorf("peek ltx header: %w", err)
	}

	// Exit if LTX file does already exists.
	path := db.LTXPath(r.Header().MinTXID, r.Header().MaxTXID)
	if _, err := os.Stat(path); err == nil {
		log.Printf("ltx file already exists, skipping: %s", path)
		return nil
	}

	// Verify LTX file pre-apply checksum matches the current database position
	// unless this is a snapshot, which will overwrite all data.
	if hdr := r.Header(); !hdr.IsSnapshot() {
		expectedPos := Pos{
			TXID:              r.Header().MinTXID - 1,
			PostApplyChecksum: r.Header().PreApplyChecksum,
		}
		if pos := db.Pos(); pos != expectedPos {
			return fmt.Errorf("position mismatch on db %q: %s <> %s", db.Name(), pos, expectedPos)
		}
	}

	// TODO: Remove all LTX files if this is a snapshot.

	// Write LTX file to a temporary file and we'll atomically rename later.
	tmpPath := path + ".tmp"
	defer func() { _ = os.Remove(tmpPath) }()

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create temp ltx file: %w", err)
	}
	defer func() { _ = f.Close() }()

	n, err := io.Copy(f, r)
	if err != nil {
		return fmt.Errorf("write ltx file: %w", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync ltx file: %w", err)
	}

	// Atomically rename file.
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename ltx file: %w", err)
	} else if err := internal.Sync(filepath.Dir(path)); err != nil {
		return fmt.Errorf("sync ltx dir: %w", err)
	}

	log.Printf("recv frame<ltx>: db=%q tx=%s-%s size=%d", db.Name(), ltx.FormatTXID(r.Header().MinTXID), ltx.FormatTXID(r.Header().MaxTXID), n)

	// Update metrics
	dbLTXCountMetricVec.WithLabelValues(db.Name()).Inc()
	dbLTXBytesMetricVec.WithLabelValues(db.Name()).Set(float64(n))

	// Attempt to apply the LTX file to the database.
	if err := db.ApplyLTX(ctx, path); err != nil {
		return fmt.Errorf("apply ltx: %w", err)
	}

	return nil
}

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

		m.DBs[db.Name()] = &dbVarJSON{
			Name:     db.Name(),
			PageSize: db.PageSize(),
			TXID:     ltx.FormatTXID(pos.TXID),
			Checksum: fmt.Sprintf("%016x", pos.PostApplyChecksum),

			PendingLock:  db.pendingLock.State().String(),
			SharedLock:   db.sharedLock.State().String(),
			ReservedLock: db.reservedLock.State().String(),
		}
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

// Subscriber subscribes to changes to databases in the store.
//
// It implements a set of "dirty" databases instead of a channel of all events
// as clients can be slow and we don't want to cause channels to back up. It
// is the responsibility of the caller to determine the state changes which is
// usually just checking the position of the client versus the store's database.
type Subscriber struct {
	store *Store

	mu       sync.Mutex
	notifyCh chan struct{}
	dirtySet map[string]struct{}
}

// newSubscriber returns a new instance of Subscriber associated with a store.
func newSubscriber(store *Store) *Subscriber {
	s := &Subscriber{
		store:    store,
		notifyCh: make(chan struct{}, 1),
		dirtySet: make(map[string]struct{}),
	}
	return s
}

// Close removes the subscriber from the store.
func (s *Subscriber) Close() error {
	s.store.Unsubscribe(s)
	return nil
}

// NotifyCh returns a channel that receives a value when the dirty set has changed.
func (s *Subscriber) NotifyCh() <-chan struct{} { return s.notifyCh }

// MarkDirty marks a database ID as dirty.
func (s *Subscriber) MarkDirty(name string) {
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
func (s *Subscriber) DirtySet() map[string]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirtySet := s.dirtySet
	s.dirtySet = make(map[string]struct{})
	return dirtySet
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
)
