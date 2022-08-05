package litefs

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/superfly/ltx"
	"golang.org/x/sync/errgroup"
)

// Store represents a collection of databases.
type Store struct {
	mu   sync.Mutex
	path string

	nextDBID    uint32
	dbsByID     map[uint32]*DB
	dbsByName   map[string]*DB
	subscribers map[*Subscriber]struct{}

	isPrimary  bool   // if true, store is current primary
	primaryURL string // if non-blank, contains the advertise URL of the current primary
	isPrimaryCandidate bool // if true, we are eligible to become the primary

	ctx    context.Context
	cancel func()
	g      errgroup.Group

	// Client used to connect to other LiteFS instances.
	Client Client

	// Leaser manages the lease that controls leader election.
	Leaser Leaser

	// Callback to notify kernel of file changes.
	Invalidator Invalidator
}

// NewStore returns a new instance of Store.
func NewStore(path string, isPrimaryCandidate bool) *Store {
	s := &Store{
		path:     path,
		nextDBID: 1,

		dbsByID:   make(map[uint32]*DB),
		dbsByName: make(map[string]*DB),

		subscribers: make(map[*Subscriber]struct{}),
		isPrimaryCandidate: isPrimaryCandidate,
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	return s
}

// Path returns underlying data directory.
func (s *Store) Path() string { return s.path }

// DBDir returns the folder that stores a single database.
func (s *Store) DBDir(id uint32) string {
	return filepath.Join(s.path, FormatDBID(id))
}

// Open initializes the store based on files in the data directory.
func (s *Store) Open() error {
	if err := os.MkdirAll(s.path, 0777); err != nil {
		return err
	}

	if err := s.openDatabases(); err != nil {
		return fmt.Errorf("open databases: %w", err)
	}

	// Begin background replication monitor.
	if s.Leaser != nil {
		s.g.Go(func() error { return s.monitor(s.ctx) })
	} else {
		log.Printf("WARNING: no leaser assigned, running as defacto primary (for testing only)")
		s.isPrimary = true
	}

	return nil
}

func (s *Store) openDatabases() error {
	f, err := os.Open(s.path)
	if err != nil {
		return fmt.Errorf("open data dir: %w", err)
	}
	defer f.Close()

	fis, err := f.Readdir(-1)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}
	for _, fi := range fis {
		dbID, err := ParseDBID(fi.Name())
		if err != nil {
			log.Printf("not a database directory, skipping: %q", fi.Name())
			continue
		} else if err := s.openDatabase(dbID); err != nil {
			return fmt.Errorf("open database: db=%s err=%w", FormatDBID(dbID), err)
		}
	}

	return nil
}

func (s *Store) openDatabase(id uint32) error {
	// Instantiate and open database.
	db := NewDB(s, id, s.DBDir(id))
	if err := db.Open(); err != nil {
		return err
	}

	// Add to internal lookups.
	s.dbsByID[id] = db
	s.dbsByName[db.Name()] = db

	// Ensure next DBID is higher than DB's id
	if s.nextDBID <= id {
		s.nextDBID = id + 1
	}

	return nil
}

// Close signals for the store to shut down.
func (s *Store) Close() error {
	s.cancel()
	return s.g.Wait()
}

// IsPrimary returns true if store has a lease to be the primary.
func (s *Store) IsPrimary() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isPrimary
}

// PrimaryURL returns the advertising URL of the current primary.
func (s *Store) PrimaryURL() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.primaryURL
}

// IsPrimaryCandidate returns true if store is eligible to be the primary.
func (s *Store) IsPrimaryCandidate() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isPrimaryCandidate
}

// DB returns a database by ID. Returns nil if the database does not exist.
func (s *Store) DB(id uint32) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dbsByID[id]
}

// DBByName returns a database by name.
// Returns nil if the database does not exist.
func (s *Store) DBByName(name string) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dbsByName[name]
}

// DBs returns a list of databases.
func (s *Store) DBs() []*DB {
	s.mu.Lock()
	defer s.mu.Unlock()

	a := make([]*DB, 0, len(s.dbsByID))
	for _, db := range s.dbsByID {
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
	if _, ok := s.dbsByName[name]; ok {
		return nil, nil, ErrDatabaseExists
	}

	// Generate next available ID.
	id := s.nextDBID
	s.nextDBID++

	// Generate database directory with name file & empty database file.
	dbDir := s.DBDir(id)
	if err := os.MkdirAll(dbDir, 0777); err != nil {
		return nil, nil, err
	} else if err := os.WriteFile(filepath.Join(dbDir, "name"), []byte(name), 0666); err != nil {
		return nil, nil, err
	}

	f, err := os.OpenFile(filepath.Join(dbDir, "database"), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
	if err != nil {
		return nil, nil, err
	}

	// Create new database instance and add to maps.
	db := NewDB(s, id, dbDir)
	if err := db.Open(); err != nil {
		f.Close()
		return nil, nil, err
	}
	s.dbsByID[id] = db
	s.dbsByName[name] = db

	// Notify listeners of change.
	s.markDirty(id)

	return db, f, nil
}

// ForceCreateDB creates a database with the given ID & name.
// This occurs when replicating from a primary server.
func (s *Store) ForceCreateDB(id uint32, name string) (*DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Exit if database with same name already exists.
	if db := s.dbsByID[id]; db != nil && db.Name() == name {
		return db, nil
	}

	// TODO: Handle conflict if another database exists with the same name.

	// Generate database directory with name file & empty database file.
	dbDir := s.DBDir(id)
	if err := os.MkdirAll(dbDir, 0777); err != nil {
		return nil, err
	} else if err := os.WriteFile(filepath.Join(dbDir, "name"), []byte(name), 0666); err != nil {
		return nil, err
	}

	if err := os.WriteFile(filepath.Join(dbDir, "database"), nil, 0666); err != nil {
		return nil, err
	}

	// Create new database instance and add to maps.
	db := NewDB(s, id, dbDir)
	if err := db.Open(); err != nil {
		return nil, err
	}
	s.dbsByID[id] = db
	s.dbsByName[name] = db

	// Notify listeners of change.
	s.markDirty(id)

	return db, nil
}

// PosMap returns a map of databases and their transactional position.
func (s *Store) PosMap() map[uint32]Pos {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := make(map[uint32]Pos, len(s.dbsByID))
	for _, db := range s.dbsByID {
		m[db.ID()] = db.Pos()
	}
	return m
}

// Subscribe creates a new subscriber for store changes.
func (s *Store) Subscribe() *Subscriber {
	s.mu.Lock()
	defer s.mu.Unlock()
	sub := newSubscriber(s)
	s.subscribers[sub] = struct{}{}
	return sub
}

// Unsubscribe removes a subscriber from the store.
func (s *Store) Unsubscribe(sub *Subscriber) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.subscribers, sub)
}

// MarkDirty marks a database ID dirty on all subscribers.
func (s *Store) MarkDirty(dbID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markDirty(dbID)
}

func (s *Store) markDirty(dbID uint32) {
	for sub := range s.subscribers {
		sub.MarkDirty(dbID)
	}
}

// monitor continuously handles either the leader lease or replicates from the primary.
func (s *Store) monitor(ctx context.Context) error {
	for {
		// Exit if store is closed.
		if err := ctx.Err(); err != nil {
			return nil
		}

		// Attempt to either obtain a primary lock or read the current primary.
		lease, primaryURL, err := s.acquireLeaseOrPrimaryURL(ctx)
		if err != nil {
			log.Printf("cannot acquire lease or find primary, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Monitor as primary if we have obtained a lease.
		if lease != nil {
			log.Printf("primary lease acquired, advertising as %s", s.Leaser.AdvertiseURL())
			if err := s.monitorAsPrimary(ctx, lease); err != nil {
				log.Printf("primary lease lost, retrying: %s", err)
			}
			continue
		}

		// Monitor as replica if another primary already exists.
		log.Printf("existing primary found (%s), connecting as replica", primaryURL)
		if err := s.monitorAsReplica(ctx, primaryURL); err != nil {
			log.Printf("replica disconected, retrying: %s", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func (s *Store) acquireLeaseOrPrimaryURL(ctx context.Context) (Lease, string, error) {
	// Attempt to find an existing primary first.
	primaryURL, err := s.Leaser.PrimaryURL(ctx)
	if err != nil && err != ErrNoPrimary {
		return nil, "", fmt.Errorf("fetch primary url: %w", err)
	} else if primaryURL != "" {
		return nil, primaryURL, nil
	}

    // If there's no primary and we're not allowed to become the primary
    //  return an error
    if !s.IsPrimaryCandidate() {
        log.Print("no primary found and I'm not eligible")
        return nil, "", ErrNoPrimary
    }

	// If no primary, attempt to become primary.
	lease, err := s.Leaser.Acquire(ctx)
	if err != nil && err != ErrPrimaryExists {
		return nil, "", fmt.Errorf("acquire lease: %w", err)
	} else if lease != nil {
		return lease, "", nil
	}

	// If we raced to become primary and another node beat us, retry the fetch.
	primaryURL, err = s.Leaser.PrimaryURL(ctx)
	if err != nil {
		return nil, "", err
	}
	return nil, primaryURL, nil
}

// monitorAsPrimary monitors & renews the current lease.
// NOTE: This code is borrowed from the consul/api's RenewPeriodic() implementation.
func (s *Store) monitorAsPrimary(ctx context.Context, lease Lease) error {
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
	s.isPrimary = true
	s.mu.Unlock()

	// Ensure that we are no longer marked as primary once we exit this function.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.isPrimary = false
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

// monitorAsReplica tries to connect to the primary node and stream down changes.
func (s *Store) monitorAsReplica(ctx context.Context, primaryURL string) error {
	// Store the URL of the primary while we're in this function.
	s.mu.Lock()
	s.primaryURL = primaryURL
	s.mu.Unlock()

	// Clear the primary URL once we leave this function since we can no longer connect.
	defer func() {
		s.mu.Lock()
		defer s.mu.Unlock()
		s.primaryURL = ""
	}()

	posMap := s.PosMap()
	st, err := s.Client.Stream(ctx, primaryURL, posMap)
	if err != nil {
		return fmt.Errorf("connect to primary: %s", err)
	}

	for {
		frame, err := st.NextFrame()
		if err == io.EOF {
			return nil // clean disconnect
		} else if err != nil {
			return fmt.Errorf("next frame: %w", err)
		}

		switch frame := frame.(type) {
		case *DBStreamFrame:
			if err := s.processDBStreamFrame(ctx, frame); err != nil {
				return fmt.Errorf("process db stream frame: %w", err)
			}
		case *LTXStreamFrame:
			if err := s.processLTXStreamFrame(ctx, frame, st); err != nil {
				return fmt.Errorf("process ltx stream frame: %w", err)
			}
		default:
			return fmt.Errorf("invalid stream frame type: 0x%02x", frame.Type())
		}
	}
}

func (s *Store) processDBStreamFrame(ctx context.Context, frame *DBStreamFrame) error {
	log.Printf("recv frame<db>: id=%d name=%q", frame.DBID, frame.Name)
	if _, err := s.ForceCreateDB(frame.DBID, frame.Name); err != nil {
		return fmt.Errorf("force create db: id=%d err=%w", frame.DBID, err)
	}
	return nil
}

func (s *Store) processLTXStreamFrame(ctx context.Context, frame *LTXStreamFrame, r io.Reader) error {
	// Parse header.
	buf := make([]byte, ltx.HeaderSize)
	var hdr ltx.Header
	if _, err := io.ReadFull(r, buf); err != nil {
		return fmt.Errorf("read header: %w", err)
	} else if err := hdr.UnmarshalBinary(buf); err != nil {
		return fmt.Errorf("unmarshal header: %w", err)
	}

	// Look up database.
	db := s.DB(hdr.DBID)
	if db == nil {
		return fmt.Errorf("database not found: %s", FormatDBID(hdr.DBID))
	}

	// Exit if LTX file does already exists.
	path := db.LTXPath(hdr.MinTXID, hdr.MaxTXID)
	if _, err := os.Stat(path); err == nil {
		log.Printf("ltx file already exists, skipping: %s", path)
		return nil
	}

	log.Printf("recv frame<ltx>: db=%d tx=(%d,%d) size=%d", hdr.DBID, hdr.MinTXID, hdr.MaxTXID, frame.Size)

	// Write LTX file to a temporary file and we'll atomically rename later.
	tmpPath := path + ".tmp"
	defer os.Remove(tmpPath)

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create temp ltx file: %w", err)
	}
	defer f.Close()

	// Write LTX contents.
	if _, err := f.Write(buf); err != nil {
		return fmt.Errorf("write ltx header: %w", err)
	} else if _, err := io.CopyN(f, r, frame.Size-int64(len(buf))); err != nil {
		return fmt.Errorf("write ltx file: %w", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("fsync ltx file: %w", err)
	}

	// Atomically rename file.
	if err := os.Rename(tmpPath, path); err != nil {
		return fmt.Errorf("rename ltx file: %w", err)
	}

	// Attempt to apply the LTX file to the database.
	if err := db.TryApplyLTX(path); err != nil {
		return fmt.Errorf("apply ltx: %w", err)
	}

	return nil
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
	dirtySet map[uint32]struct{}
}

// newSubscriber returns a new instance of Subscriber associated with a store.
func newSubscriber(store *Store) *Subscriber {
	s := &Subscriber{
		store:    store,
		notifyCh: make(chan struct{}, 1),
		dirtySet: make(map[uint32]struct{}),
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
func (s *Subscriber) MarkDirty(dbID uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.dirtySet[dbID] = struct{}{}

	select {
	case s.notifyCh <- struct{}{}:
	default:
	}
}

// DirtySet returns a set of database IDs that have changed since the last call
// to DirtySet(). This call clears the set.
func (s *Subscriber) DirtySet() map[uint32]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirtySet := s.dirtySet
	s.dirtySet = make(map[uint32]struct{})
	return dirtySet
}
