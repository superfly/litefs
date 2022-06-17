package litefs

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// Store represents a collection of databases.
type Store struct {
	mu        sync.Mutex
	path      string
	nextDBID  uint64
	dbsByID   map[uint64]*DB
	dbsByName map[string]*DB
}

// NewStore returns a new instance of Store.
func NewStore(path string) *Store {
	return &Store{
		path:     path,
		nextDBID: 1,

		dbsByID:   make(map[uint64]*DB),
		dbsByName: make(map[string]*DB),
	}
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
	return nil
}

func (s *Store) openDatabases() error {
	panic("TODO: Iterate over folders in data directory to open database folders.")
}

func (s *Store) openDatabase(id int64) error {
	panic("TODO: Instantiate DB")
	panic("TODO: Insert into maps")
	panic("TODO: Ensure next DBID is higher than DB's id")
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

// CreateDB creates a new database with the given name.
// Returns an error if a database with the same name already exists.
func (s *Store) CreateDB(name string) (*DB, error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Verify database doesn't already exist.
	if _, ok := s.dbsByName[name]; ok {
		return nil, ErrDatabaseExists
	}

	// Generate next available ID.
	id := s.nextDBID
	s.nextDBID++

	// Generate database directory with name file & empty database file.
	dbDir := s.DBDir(id)
	if err := os.MkdirAll(dbDir, 0777); err != nil {
		return nil, err
	} else if err := os.WriteFile(filepath.Join(dbDir, "name"), []byte(name), 0777); err != nil {
		return nil, err
	} else if err := os.WriteFile(filepath.Join(dbDir, "database"), nil, 0777); err != nil {
		return nil, err
	}

	// Create new database instance and add to maps.
	db := NewDB(id, dbDir)
	if err := db.Open(); err != nil {
		return nil, err
	}
	s.dbsByID[id] = db
	s.dbsByName[name] = db

	// Notify listeners of change.
	s.broadcast(id)

	return db, nil
}

func (s *Store) broadcast(dbID uint64) {
	// TODO: Notify subscribers of change to a database
}
