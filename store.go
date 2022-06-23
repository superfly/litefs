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
	mu          sync.Mutex
	path        string
	nextDBID    uint64
	dbsByID     map[uint64]*DB
	dbsByName   map[string]*DB
	subscribers map[*Subscriber]struct{}

	ctx    context.Context
	cancel func()
	g      errgroup.Group

	// TEMP: Primary node URL
	PrimaryURL string

	// Client used to connect to other LiteFS instances.
	Client Client
}

// NewStore returns a new instance of Store.
func NewStore(path string) *Store {
	s := &Store{
		path:     path,
		nextDBID: 1,

		dbsByID:   make(map[uint64]*DB),
		dbsByName: make(map[string]*DB),

		subscribers: make(map[*Subscriber]struct{}),
	}
	s.ctx, s.cancel = context.WithCancel(context.Background())

	return s
}

// Path returns underlying data directory.
func (s *Store) Path() string { return s.path }

// DBDir returns the folder that stores a single database.
func (s *Store) DBDir(id uint64) string {
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
	s.g.Go(func() error { return s.monitor(s.ctx) })

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
			continue
		} else if err := s.openDatabase(dbID); err != nil {
			return fmt.Errorf("open database: db=%s err=%w", FormatDBID(dbID), err)
		}
	}

	return nil
}

func (s *Store) openDatabase(id uint64) error {
	// Instantiate and open database.
	db := NewDB(s, id, s.DBDir(id))
	if err := db.Open(); err != nil {
		return err
	}

	println("dbg/db", db.id, db.name, db.pos.TXID)

	// Add to internal lookups.
	s.dbsByID[id] = db
	s.dbsByName[db.Name()] = db

	// Ensure next DBID is higher than DB's id
	if s.nextDBID <= id {
		s.nextDBID = id + 1
	}

	return nil
}

// FindDB returns a database by ID. Returns nil if the database does not exist.
func (s *Store) FindDB(id uint64) *DB {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.dbsByID[id]
}

// FindDBByName returns a database by name.
// Returns nil if the database does not exist.
func (s *Store) FindDBByName(name string) *DB {
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

// PosMap returns a map of databases and their transactional position.
func (s *Store) PosMap() map[uint64]Pos {
	s.mu.Lock()
	defer s.mu.Unlock()

	m := make(map[uint64]Pos, len(s.dbsByID))
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
func (s *Store) MarkDirty(dbID uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.markDirty(dbID)
}

func (s *Store) markDirty(dbID uint64) {
	for sub := range s.subscribers {
		sub.MarkDirty(dbID)
	}
}

// monitor continuously tries to connect to the primary node and stream down changes.
func (s *Store) monitor(ctx context.Context) error {
	if s.PrimaryURL == "" { // TEMP
		return nil
	}

	for {
		if err := s.stream(ctx); err != nil {
			log.Printf("stream error, retrying: %s", err)
			time.Sleep(1 * time.Second)
			continue
		}

		// Exit once the store has been closed.
		if ctx.Err() != nil {
			return nil
		}
	}
}

func (s *Store) stream(ctx context.Context) error {
	posMap := s.PosMap()
	st, err := s.Client.Stream(ctx, s.PrimaryURL, posMap)
	if err != nil {
		return fmt.Errorf("connect: %s", err)
	}

	for {
		frame, err := st.NextFrame()
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
		default:
			return fmt.Errorf("invalid stream frame type: 0x%02x", frame.Type())
		}
	}
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

	log.Printf("dbg/ltx: db=%s txid=(%s,%s)", hdr.DBID, hdr.MinTXID, hdr.MaxTXID)

	// Look up database.
	db := s.FindDB(hdr.DBID)
	if db == nil {
		return fmt.Errorf("database not found: %s", FormatDBID(hdr.DBID))
	}

	// Exit if LTX file does already exists.
	path := db.LTXPath(hdr.MinTXID, hdr.MaxTXID)
	if _, err := os.Stat(path); err == nil {
		log.Printf("ltx file already exists, skipping: %s", path)
		return nil
	}

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
	dirtySet map[uint64]struct{}
}

// newSubscriber returns a new instance of Subscriber associated with a store.
func newSubscriber(store *Store) *Subscriber {
	s := &Subscriber{
		store:    store,
		notifyCh: make(chan struct{}, 1),
		dirtySet: make(map[uint64]struct{}),
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
func (s *Subscriber) MarkDirty(dbID uint64) {
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
func (s *Subscriber) DirtySet() map[uint64]struct{} {
	s.mu.Lock()
	defer s.mu.Unlock()

	dirtySet := s.dirtySet
	s.dirtySet = make(map[uint64]struct{})
	return dirtySet
}
