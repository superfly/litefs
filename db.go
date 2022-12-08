package litefs

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/ltx"
)

// DBMode represents either a rollback journal or WAL mode.
type DBMode int

// Database journal modes.
const (
	DBModeRollback = DBMode(0)
	DBModeWAL      = DBMode(1)
)

type State int

func (s State) String() string {
	switch s {
	case StateUnlocked:
		return "UNLOCKED"
	case StateJournalReading:
		return "JOURNAL:READING"
	case StateJournalWriting:
		return "JOURNAL:WRITING"
	default:
		return fmt.Sprintf("State<%d>", s)
	}
}

const (
	StateUnlocked       = State(0x00) // no locks acquired
	StateJournalReading = State(0x01) // one or more readers in rollback journal mode
	StateJournalWriting = State(0x02) // one writer in rollback journal mode
)

// DB represents a SQLite database.
type DB struct {
	store    *Store       // parent store
	name     string       // name of database
	path     string       // full on-disk path
	state    atomic.Value // fsm state
	pageSize uint32       // database page size, if known
	pageN    uint32       // database size, in pages
	pos      atomic.Value // current tx position (Pos)
	mode     DBMode       // database journaling mode (rollback, wal)

	chksums        map[uint32]uint64   // database page checksums
	journalChksums map[uint32]uint64   // journal frame page checksums
	walChksums     map[uint32][]uint64 // wal page checksums

	dirtyPageSet map[uint32]struct{}

	wal struct {
		offset           int64            // offset of the start of the transaction
		byteOrder        binary.ByteOrder // determine by WAL header magic
		salt1, salt2     uint32           // current WAL header salt values
		chksum1, chksum2 uint32           // WAL checksum values at wal.offset
		frameOffsets     map[uint32]int64 // WAL frame offset of the last version of a given pgno before current tx
	}

	// Collection of outstanding guard sets, protected by a mutex.
	guardSets struct {
		mu sync.Mutex
		m  map[uint64]*GuardSet
	}

	// SQLite database locks
	pendingLock  RWMutex
	sharedLock   RWMutex
	reservedLock RWMutex

	// SQLite WAL locks
	writeLock   RWMutex
	ckptLock    RWMutex
	recoverLock RWMutex
	read0Lock   RWMutex
	read1Lock   RWMutex
	read2Lock   RWMutex
	read3Lock   RWMutex
	read4Lock   RWMutex
	dmsLock     RWMutex

	// Returns the current time. Used for mocking time in tests.
	Now func() time.Time
}

// NewDB returns a new instance of DB.
func NewDB(store *Store, name string, path string) *DB {
	db := &DB{
		store: store,
		name:  name,
		path:  path,

		chksums:        make(map[uint32]uint64),
		journalChksums: make(map[uint32]uint64),
		walChksums:     make(map[uint32][]uint64),

		dirtyPageSet: make(map[uint32]struct{}),

		Now: time.Now,
	}
	db.pos.Store(Pos{})
	db.state.Store(StateUnlocked)
	db.wal.frameOffsets = make(map[uint32]int64)
	db.guardSets.m = make(map[uint64]*GuardSet)

	return db
}

// Name of the database name.
func (db *DB) Name() string { return db.name }

// Store returns the store that the database is a member of.
func (db *DB) Store() *Store { return db.store }

// Path of the database's data directory.
func (db *DB) Path() string { return db.path }

// LTXDir returns the path to the directory of LTX transaction files.
func (db *DB) LTXDir() string { return filepath.Join(db.path, "ltx") }

// LTXPath returns the path of an LTX file.
func (db *DB) LTXPath(minTXID, maxTXID uint64) string {
	return filepath.Join(db.LTXDir(), ltx.FormatFilename(minTXID, maxTXID))
}

// ReadLTXDir returns DirEntry for every LTX file.
func (db *DB) ReadLTXDir() ([]fs.DirEntry, error) {
	ents, err := os.ReadDir(db.LTXDir())
	if os.IsNotExist(err) {
		return nil, nil
	} else if err != nil {
		return nil, fmt.Errorf("readdir: %w", err)
	}

	for i := 0; i < len(ents); i++ {
		if _, _, err := ltx.ParseFilename(ents[i].Name()); err != nil {
			ents, i = append(ents[:i], ents[i+1:]...), i-1
		}
	}

	// Ensure results are in sorted order.
	sort.Slice(ents, func(i, j int) bool { return ents[i].Name() < ents[j].Name() })

	return ents, nil
}

// DatabasePath returns the path to the underlying database file.
func (db *DB) DatabasePath() string { return filepath.Join(db.path, "database") }

// JournalPath returns the path to the underlying journal file.
func (db *DB) JournalPath() string { return filepath.Join(db.path, "journal") }

// WALPath returns the path to the underlying WAL file.
func (db *DB) WALPath() string { return filepath.Join(db.path, "wal") }

// SHMPath returns the path to the underlying shared memory file.
func (db *DB) SHMPath() string { return filepath.Join(db.path, "shm") }

// Pos returns the current transaction position of the database.
func (db *DB) Pos() Pos {
	return db.pos.Load().(Pos)
}

// setPos sets the current transaction position of the database.
func (db *DB) setPos(pos Pos) error {
	db.pos.Store(pos)

	// Invalidate page cache.
	if invalidator := db.store.Invalidator; invalidator != nil {
		if err := invalidator.InvalidatePos(db); err != nil {
			return fmt.Errorf("invalidate pos: %w", err)
		}
	}

	// Update metrics.
	dbTXIDMetricVec.WithLabelValues(db.name).Set(float64(pos.TXID))

	return nil
}

// TXID returns the current transaction ID.
func (db *DB) TXID() uint64 { return db.Pos().TXID }

// State returns the current FSM state of the database.
func (db *DB) State() State {
	return db.state.Load().(State)
}

func (db *DB) setState(state State) {
	TraceLog.Printf("[TRANSITION(%s)]: %s => %s", db.name, db.State(), state)
	db.state.Store(state)
}

// Open initializes the database from files in its data directory.
func (db *DB) Open() error {
	// Read page size & page count from database file.
	if err := db.initFromDatabaseHeader(); err != nil {
		return fmt.Errorf("init from database header: %w", err)
	}

	// Ensure "ltx" directory exists.
	if err := os.MkdirAll(db.LTXDir(), 0777); err != nil {
		return err
	}

	// Remove all SHM files on start up.
	if err := os.Remove(db.SHMPath()); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("remove shm: %w", err)
	}

	// If a journal file exists, rollback the last transaction so that we
	// are in a consistent state before applying our last LTX file. Otherwise
	// we could have a partial transaction in our database, apply our LTX, and
	// then the SQLite client will recover from the journal and corrupt everything.
	//
	// See: https://github.com/superfly/litefs/issues/134
	if err := db.rollbackJournal(context.Background()); err != nil {
		return fmt.Errorf("rollback journal: %w", err)
	}

	// Copy the WAL file back to the main database. This ensures that we can
	// compute the checksum only using the database file instead of having to
	// first compute the latest page set from the WAL to overlay.
	if err := db.checkpoint(context.Background()); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
	}

	if err := db.recoverFromLTX(); err != nil {
		return fmt.Errorf("recover ltx: %w", err)
	}

	// Validate database file.
	if err := db.verifyDatabaseFile(); err != nil {
		return fmt.Errorf("verify database file: %w", err)
	}

	return nil
}

// initFromDatabaseHeader reads the page size & page count from the database file header.
func (db *DB) initFromDatabaseHeader() error {
	f, err := os.Open(db.DatabasePath())
	if os.IsNotExist(err) {
		return nil // no database file yet, skip
	} else if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Read page size into memory.
	hdr, _, err := readSQLiteDatabaseHeader(f)
	if err == io.EOF {
		return nil
	} else if err == errInvalidDatabaseHeader { // invalid file
		log.Printf("invalid database header on %q, clearing data files", db.name)
		if err := db.clean(); err != nil {
			return fmt.Errorf("clean: %w", err)
		}
		return nil
	} else if err != nil {
		return err
	}
	db.pageSize = hdr.PageSize
	db.pageN = hdr.PageN

	// Initialize database mode.
	if hdr.WriteVersion == 2 && hdr.ReadVersion == 2 {
		db.mode = DBModeWAL
	} else {
		db.mode = DBModeRollback
	}

	return nil
}

// rollbackJournal copies all the pages from an existing rollback journal back
// to the database file. This is called on startup so that we can be in a
// consistent state in order to verify our checksums.
func (db *DB) rollbackJournal(ctx context.Context) error {
	journalFile, err := os.OpenFile(db.JournalPath(), os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		return nil // no journal file, skip
	} else if err != nil {
		return err
	}
	defer func() { _ = journalFile.Close() }()

	dbFile, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666)
	if err != nil {
		return err
	}
	defer func() { _ = dbFile.Close() }()

	// Copy every journal page back into the main database file.
	r := NewJournalReader(journalFile, db.pageSize)
	for i := 0; ; i++ {
		if err := r.Next(); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("next segment(%d): %w", i, err)
		}
		if err := db.rollbackJournalSegment(ctx, r, dbFile); err != nil {
			return fmt.Errorf("segment(%d): %w", i, err)
		}
	}

	// Resize database to size before journal transaction.
	if err := dbFile.Truncate(r.DatabaseSize()); err != nil {
		return err
	}

	if err := dbFile.Sync(); err != nil {
		return err
	} else if err := dbFile.Close(); err != nil {
		return err
	} else if err := journalFile.Close(); err != nil {
		return err
	}

	if err := os.Remove(db.JournalPath()); err != nil {
		return err
	}

	return nil
}

func (db *DB) rollbackJournalSegment(ctx context.Context, r *JournalReader, dbFile *os.File) error {
	for i := 0; ; i++ {
		pgno, data, err := r.ReadFrame()
		if err == io.EOF {
			return nil
		} else if err != nil {
			return fmt.Errorf("read frame(%d): %w", i, err)
		}

		// Write data to the database file.
		if _, err := dbFile.WriteAt(data, int64(pgno-1)*int64(db.pageSize)); err != nil {
			return fmt.Errorf("write to database (pgno=%d): %w", pgno, err)
		}
	}
}

func (db *DB) checkpoint(ctx context.Context) error {
	// Open the database file we'll checkpoint into. Skip if this hasn't been created.
	dbFile, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		return nil // no database file yet, skip
	} else if err != nil {
		return err
	}
	defer func() { _ = dbFile.Close() }()

	// Open the WAL file that we'll copy from. Skip if it was cleanly closed and removed.
	walFile, err := os.Open(db.WALPath())
	if os.IsNotExist(err) {
		return nil // no WAL file, skip
	} else if err != nil {
		return err
	}
	defer func() { _ = walFile.Close() }()

	offsets, commit, err := db.readWALPageOffsets(walFile)
	if err != nil {
		return fmt.Errorf("read wal page offsets: %w", err)
	}

	// Copy pages from the WAL to the main database file & resize db file.
	if len(offsets) > 0 {
		buf := make([]byte, db.pageSize)
		for pgno, offset := range offsets {
			if _, err := walFile.Seek(offset+WALFrameHeaderSize, io.SeekStart); err != nil {
				return fmt.Errorf("seek wal: %w", err)
			} else if _, err := io.ReadFull(walFile, buf); err != nil {
				return fmt.Errorf("read wal: %w", err)
			}

			if _, err := dbFile.WriteAt(buf, int64(pgno-1)*int64(db.pageSize)); err != nil {
				return fmt.Errorf("write db page %d: %w", pgno, err)
			}
		}

		if err := dbFile.Truncate(int64(commit) * int64(db.pageSize)); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}

		// Save the size of the database, in pages, based on last commit.
		db.pageN = commit
	}

	// Remove WAL file.
	if err := os.Remove(db.WALPath()); err != nil {
		return fmt.Errorf("remove wal: %w", err)
	}

	return nil
}

// readWALPageOffsets returns a map of the offsets of the last committed version
// of each page in the WAL. Also returns the commit size of the last transaction.
func (db *DB) readWALPageOffsets(f *os.File) (_ map[uint32]int64, lastCommit uint32, _ error) {
	r := NewWALReader(f)
	if err := r.ReadHeader(); err == io.EOF {
		return nil, 0, nil
	}

	// Read the offset of the last version of each page in the WAL.
	offsets := make(map[uint32]int64)
	txOffsets := make(map[uint32]int64)
	buf := make([]byte, r.PageSize())
	for {
		pgno, commit, err := r.ReadFrame(buf)
		if err == io.EOF {
			break
		} else if err != nil {
			return nil, 0, err
		}

		// Save latest offset for each page version.
		txOffsets[pgno] = r.Offset()

		// If this is not a committing frame, continue to next frame.
		if commit == 0 {
			continue
		}

		// At the end of each transaction, copy offsets to main map.
		lastCommit = commit
		for k, v := range txOffsets {
			offsets[k] = v
		}
		txOffsets = make(map[uint32]int64)
	}

	return offsets, lastCommit, nil
}

// recoverFromLTX finds the last LTX file to determine the TXID and then
// re-applies the last file in case transaction was lost from the journal/WAL.
func (db *DB) recoverFromLTX() error {
	fis, err := os.ReadDir(db.LTXDir())
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}

	var pos Pos
	var filename string
	for _, fi := range fis {
		minTXID, maxTXID, err := ltx.ParseFilename(fi.Name())
		if err != nil {
			continue
		}

		// Read header to find the checksum for the transaction.
		header, trailer, err := readAndVerifyLTXFile(filepath.Join(db.LTXDir(), fi.Name()))
		if err != nil {
			return fmt.Errorf("read ltx file header (%s): %w", fi.Name(), err)
		}

		// Ensure header TXIDs match the filename.
		if header.MinTXID != minTXID || header.MaxTXID != maxTXID {
			return fmt.Errorf("ltx header txid (%s,%s) does not match filename (%s)", ltx.FormatTXID(header.MaxTXID), ltx.FormatTXID(maxTXID), fi.Name())
		}

		// Save latest position.
		if header.MaxTXID > pos.TXID {
			pos = Pos{
				TXID:              header.MaxTXID,
				PostApplyChecksum: trailer.PostApplyChecksum,
			}
			filename = fi.Name()
		}
	}

	// Ignore remaining processing if we don't have an LTX file to load from.
	if pos.IsZero() {
		return nil
	}

	// Apply the last LTX file so our checksums match if there was a failure in
	// between the LTX commit and journal/WAL commit.
	if err := db.ApplyLTX(context.Background(), filepath.Join(db.LTXDir(), filename)); err != nil {
		return fmt.Errorf("apply ltx: %w", err)
	}

	return nil
}

// verifyDatabaseFile opens and validates the database file, if it exists.
func (db *DB) verifyDatabaseFile() error {
	f, err := os.Open(db.DatabasePath())
	if os.IsNotExist(err) {
		return nil // no database file yet
	} else if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	hdr, _, err := readSQLiteDatabaseHeader(f)
	if err == io.EOF {
		return nil // no contents yet
	} else if err != nil {
		return fmt.Errorf("cannot read database header: %w", err)
	}
	db.pageSize = hdr.PageSize
	db.pageN = hdr.PageN

	assert(db.pageSize > 0, "page size must be greater than zero")

	// Calculate checksum for each page & for entire database.
	var chksum uint64
	buf := make([]byte, db.pageSize)
	db.chksums = make(map[uint32]uint64)
	for pgno := uint32(1); pgno <= db.pageN; pgno++ {
		offset := int64(pgno-1) * int64(db.pageSize)
		if _, err := internal.ReadFullAt(f, buf, offset); err != nil {
			return fmt.Errorf("read database page: %w", err)
		}

		pageChksum := ltx.ChecksumPage(pgno, buf)
		db.chksums[pgno] = pageChksum

		chksum = ltx.ChecksumFlag | (chksum ^ pageChksum)
	}

	// Ensure database checksum matches checksum in current position.
	if pos := db.Pos(); chksum != pos.PostApplyChecksum {
		return fmt.Errorf("database checksum (%016x) does not match latest LTX checksum (%016x)", chksum, pos.PostApplyChecksum)
	}

	return nil
}

// clean deletes and recreates the database data directory.
func (db *DB) clean() error {
	if err := os.RemoveAll(db.path); err != nil && !os.IsNotExist(err) {
		return err
	}
	return os.Mkdir(db.path, 0777)
}

// OpenLTXFile returns a file handle to an LTX file that contains the given TXID.
func (db *DB) OpenLTXFile(txID uint64) (*os.File, error) {
	return os.Open(db.LTXPath(txID, txID))
}

// OpenDatabase returns a handle for the database file.
func (db *DB) OpenDatabase(ctx context.Context) (*os.File, error) {
	f, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666)
	TraceLog.Printf("[OpenDatabase(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// CloseDatabase closes a handle associated with the database file.
func (db *DB) CloseDatabase(ctx context.Context, f *os.File, owner uint64) error {
	err := f.Close()
	TraceLog.Printf("[CloseDatabase(%s)]: owner=%d %s", db.name, owner, errorKeyValue(err))
	return err
}

// TruncateDatabase sets the size of the database file.
func (db *DB) TruncateDatabase(ctx context.Context, size int64) (err error) {
	defer func() {
		TraceLog.Printf("[TruncateDatabase(%s)]: size=%d %s", db.name, size, errorKeyValue(err))
	}()

	// Require the page size because we need to check against the page count & checksums.
	if db.pageSize == 0 {
		return fmt.Errorf("page size required on database truncation")
	} else if size%int64(db.pageSize) != 0 {
		return fmt.Errorf("size must be page-aligned (%d bytes)", db.pageSize)
	}

	// Verify new size matches the database size specified in the header.
	pageN := uint32(size / int64(db.pageSize))
	if pageN != db.pageN {
		return fmt.Errorf("truncation size (%d pages) does not match database header size (%d pages)", pageN, db.pageN)
	}

	// Determine previous size.
	fi, err := os.Stat(db.DatabasePath())
	if err != nil {
		return err
	}

	// Process the actual file system truncation.
	if err := os.Truncate(db.DatabasePath(), size); err != nil {
		return err
	}

	// Remove checksums after end of database on success.
	prevPageN := uint32(fi.Size() / int64(db.pageSize))
	for pgno := pageN + 1; pgno < prevPageN; pgno++ {
		pageChksum := db.chksums[pgno]
		TraceLog.Printf("[TruncatePage(%s)]: pgno=%d chksum=%016x", db.name, pgno, pageChksum)
		delete(db.chksums, pgno)
	}

	return nil
}

// SyncDatabase fsync's the database file.
func (db *DB) SyncDatabase(ctx context.Context) (err error) {
	defer func() {
		TraceLog.Printf("[SyncDatabase(%s)]: %s", db.name, errorKeyValue(err))
	}()

	f, err := os.Open(db.DatabasePath())
	if err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// ReadDatabaseAt reads from the database at the specified index.
func (db *DB) ReadDatabaseAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (int, error) {
	n, err := f.ReadAt(data, offset)

	// Compute checksum if page aligned.
	var chksum string
	var pgno uint32
	if db.pageSize != 0 && offset%int64(db.pageSize) == 0 && len(data) == int(db.pageSize) {
		pgno = uint32(offset/int64(db.pageSize)) + 1
		chksum = fmt.Sprintf("%016x", ltx.ChecksumPage(pgno, data))
	}
	TraceLog.Printf("[ReadDatabaseAt(%s)]: offset=%d size=%d pgno=%d chksum=%s owner=%d %s", db.name, offset, len(data), pgno, chksum, owner, errorKeyValue(err))

	return n, err
}

// WriteDatabaseAt writes data to the main database file at the given index.
func (db *DB) WriteDatabaseAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) error {
	// Return an error if the current process is not the leader.
	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	} else if len(data) == 0 {
		return nil
	}

	// Use page size from the write.
	if db.pageSize == 0 {
		if offset != 0 {
			return fmt.Errorf("cannot determine page size, initial offset (%d) is non-zero", offset)
		}
		hdr, _, err := readSQLiteDatabaseHeader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("cannot read sqlite database header: %w", err)
		}
		db.pageSize = hdr.PageSize
	}

	// Require that writes are a single page and are page-aligned.
	// This allows us to track per-page checksums and detect errors on commit.
	if offset%int64(db.pageSize) != 0 {
		return fmt.Errorf("database writes must be page-aligned (%d bytes)", db.pageSize)
	} else if len(data) != int(db.pageSize) {
		return fmt.Errorf("database write must be exactly one page (%d bytes)", db.pageSize)
	}

	// Track dirty pages if we are using a rollback journal. This isn't
	// necessary with the write-ahead log (WAL) since pages are appended
	// instead of overwritten. We can determine the dirty set at commit-time.
	pgno := uint32(offset/int64(db.pageSize)) + 1
	if db.mode == DBModeRollback {
		db.dirtyPageSet[pgno] = struct{}{}
	}

	// Track checksum for page write.
	prevChksum := db.chksums[pgno]
	chksum := ltx.ChecksumPage(pgno, data)
	db.chksums[pgno] = chksum

	// Callback to perform write on handle.
	_, err := f.WriteAt(data, offset)
	TraceLog.Printf("[WriteDatabaseAt(%s)]: offset=%d size=%d pgno=%d chksum=%016x prev=%016x owner=%d %s", db.name, offset, len(data), pgno, chksum, prevChksum, owner, errorKeyValue(err))
	if err != nil {
		return err
	}

	dbDatabaseWriteCountMetricVec.WithLabelValues(db.name).Inc()
	return nil
}

// UnlockDatabase unlocks all locks from the database file.
func (db *DB) UnlockDatabase(ctx context.Context, owner uint64) {
	guardSet := db.GuardSet(owner)
	if guardSet == nil {
		return
	}
	TraceLog.Printf("[UnlockDatabase(%s)]: owner=%d", db.name, owner)
	guardSet.UnlockDatabase()
}

// CreateJournal creates a new journal file on disk.
func (db *DB) CreateJournal() (*os.File, error) {
	if !db.store.IsPrimary() {
		TraceLog.Printf("[CreateJournal(%s)]: %s", db.name, errorKeyValue(ErrReadOnlyReplica))
		return nil, ErrReadOnlyReplica
	}

	f, err := os.OpenFile(db.JournalPath(), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
	TraceLog.Printf("[CreateJournal(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// OpenJournal returns a handle for the journal file.
func (db *DB) OpenJournal(ctx context.Context) (*os.File, error) {
	f, err := os.OpenFile(db.JournalPath(), os.O_RDWR, 0666)
	TraceLog.Printf("[OpenJournal(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// CloseJournal closes a handle associated with the journal file.
func (db *DB) CloseJournal(ctx context.Context, f *os.File, owner uint64) error {
	err := f.Close()
	TraceLog.Printf("[CloseJournal(%s)]: owner=%d %s", db.name, owner, errorKeyValue(err))
	return err
}

// TruncateJournal sets the size of the journal file.
func (db *DB) TruncateJournal(ctx context.Context) error {
	err := db.CommitJournal(JournalModeTruncate)
	TraceLog.Printf("[TruncateJournal(%s)]: %s", db.name, errorKeyValue(err))
	return err
}

// SyncJournal fsync's the journal file.
func (db *DB) SyncJournal(ctx context.Context) (err error) {
	defer func() {
		TraceLog.Printf("[SyncJournal(%s)]: %s", db.name, errorKeyValue(err))
	}()

	f, err := os.Open(db.JournalPath())
	if err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// RemoveJournal deletes the journal file from disk.
func (db *DB) RemoveJournal(ctx context.Context) error {
	err := db.CommitJournal(JournalModeDelete)
	TraceLog.Printf("[RemoveJournal(%s)]: %s", db.name, errorKeyValue(err))
	return err
}

// ReadJournalAt reads from the journal at the specified offset.
func (db *DB) ReadJournalAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (int, error) {
	n, err := f.ReadAt(data, offset)
	TraceLog.Printf("[ReadJournalAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
	return n, err
}

// WriteJournal writes data to the rollback journal file.
func (db *DB) WriteJournalAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) error {
	if !db.store.IsPrimary() {
		TraceLog.Printf("[WriteJournalAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(ErrReadOnlyReplica))
		return ErrReadOnlyReplica
	}

	// Set the page size on initial journal header write.
	if offset == 0 && len(data) >= SQLITE_JOURNAL_HEADER_SIZE && db.pageSize == 0 {
		db.pageSize = binary.BigEndian.Uint32(data[24:])
	}

	// Assume this is a PERSIST commit if the initial header bytes are cleared.
	if offset == 0 && len(data) == SQLITE_JOURNAL_HEADER_SIZE && isByteSliceZero(data) {
		if err := db.CommitJournal(JournalModePersist); err != nil {
			return fmt.Errorf("commit journal (PERSIST): %w", err)
		}
	}

	_, err := f.WriteAt(data, offset)

	switch {
	case offset == 0 && len(data) >= SQLITE_JOURNAL_HEADER_SIZE:
		TraceLog.Printf("[WriteJournalAt(%s)]: offset=%d size=%d magic=%x frameN=%d nonce=%d pageN=%d sector=%d pageSize=%d %s",
			db.name, offset, len(data),
			data[:8], int32(binary.BigEndian.Uint32(data[8:])),
			binary.BigEndian.Uint32(data[12:]), binary.BigEndian.Uint32(data[16:]),
			binary.BigEndian.Uint32(data[20:]), binary.BigEndian.Uint32(data[24:]),
			errorKeyValue(err))

	case len(data) == 4: // frame pgno or checksum
		TraceLog.Printf("[WriteJournalAt(%s)]: offset=%d size=%d data=%d %s", db.name, offset, len(data), binary.BigEndian.Uint32(data[0:]), errorKeyValue(err))

	default:
		TraceLog.Printf("[WriteJournalAt(%s)]: offset=%d size=%d %s", db.name, offset, len(data), errorKeyValue(err))
	}

	dbJournalWriteCountMetricVec.WithLabelValues(db.name).Inc()
	return err
}

// CreateWAL creates a new WAL file on disk.
func (db *DB) CreateWAL() (*os.File, error) {
	f, err := os.OpenFile(db.WALPath(), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
	TraceLog.Printf("[CreateWAL(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// OpenWAL returns a handle for the write-ahead log file.
func (db *DB) OpenWAL(ctx context.Context) (*os.File, error) {
	f, err := os.OpenFile(db.WALPath(), os.O_RDWR, 0666)
	TraceLog.Printf("[OpenWAL(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// CloseWAL closes a handle associated with the WAL file.
func (db *DB) CloseWAL(ctx context.Context, f *os.File, owner uint64) error {
	err := f.Close()
	TraceLog.Printf("[CloseWAL(%s)]: owner=%d %s", db.name, owner, errorKeyValue(err))
	return err
}

// TruncateWAL sets the size of the WAL file.
func (db *DB) TruncateWAL(ctx context.Context, size int64) error {
	err := os.Truncate(db.WALPath(), size)
	TraceLog.Printf("[TruncateWAL(%s)]: size=%d %s", db.name, size, errorKeyValue(err))
	return err
}

// RemoveWAL deletes the WAL file from disk.
func (db *DB) RemoveWAL(ctx context.Context) error {
	err := os.Remove(db.WALPath())
	TraceLog.Printf("[RemoveWAL(%s)]: %s", db.name, errorKeyValue(err))
	return err
}

// SyncWAL fsync's the WAL file.
func (db *DB) SyncWAL(ctx context.Context) (err error) {
	defer func() {
		TraceLog.Printf("[SyncWAL(%s)]: %s", db.name, errorKeyValue(err))
	}()

	f, err := os.Open(db.WALPath())
	if err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// ReadWALAt reads from the WAL at the specified index.
func (db *DB) ReadWALAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (int, error) {
	n, err := f.ReadAt(data, offset)
	TraceLog.Printf("[ReadWALAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
	return n, err
}

// WriteWALAt writes data to the WAL file. On final commit write, an LTX file is
// generated for the transaction.
func (db *DB) WriteWALAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (err error) {
	// Return an error if the current process is not the leader.
	if !db.store.IsPrimary() {
		TraceLog.Printf("[WriteWALAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
		return ErrReadOnlyReplica
	} else if len(data) == 0 {
		TraceLog.Printf("[WriteWALAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
		return nil
	}

	assert(db.pageSize != 0, "page size cannot be zero for wal write")

	dbWALWriteCountMetricVec.WithLabelValues(db.name).Inc()

	// Reset WAL if header is overwritten.
	if offset == 0 {
		// Require the initial header to be written fully.
		if len(data) < WALHeaderSize {
			return fmt.Errorf("short wal header write of %d bytes", len(data))
		}

		// Determine byte order of checksums.
		switch magic := binary.BigEndian.Uint32(data[0:]); magic {
		case 0x377f0682:
			db.wal.byteOrder = binary.LittleEndian
		case 0x377f0683:
			db.wal.byteOrder = binary.BigEndian
		default:
			return fmt.Errorf("invalid wal header magic: %x", magic)
		}

		// Set remaining WAL fields.
		db.wal.offset = WALHeaderSize
		db.wal.salt1 = binary.BigEndian.Uint32(data[16:])
		db.wal.salt2 = binary.BigEndian.Uint32(data[20:])
		db.wal.chksum1 = binary.BigEndian.Uint32(data[24:])
		db.wal.chksum2 = binary.BigEndian.Uint32(data[28:])
		db.wal.frameOffsets = make(map[uint32]int64)
	}

	// Passthrough write to underlying WAL file.
	_, err = f.WriteAt(data, offset)
	TraceLog.Printf("[WriteWALAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) buildTxFrameOffsets(walFile *os.File) (_ map[uint32]int64, commit, chksum1, chksum2 uint32, endOffset int64, err error) {
	m := make(map[uint32]int64)

	offset := db.wal.offset
	chksum1, chksum2 = db.wal.chksum1, db.wal.chksum2
	frame := make([]byte, WALFrameHeaderSize+int64(db.pageSize))
	for i := 0; ; i++ {
		// Read frame data & exit if we hit the end of file.
		if _, err := internal.ReadFullAt(walFile, frame, offset); err == io.EOF || err == io.ErrUnexpectedEOF {
			return nil, 0, 0, 0, 0, errNoTransaction
		} else if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("read wal frame: %w", err)
		}

		// If salt doesn't match then we've run into a previous WAL's data.
		salt1 := binary.BigEndian.Uint32(frame[8:])
		salt2 := binary.BigEndian.Uint32(frame[12:])
		if db.wal.salt1 != salt1 || db.wal.salt2 != salt2 {
			return nil, 0, 0, 0, 0, errNoTransaction
		}

		// Verify checksum
		fchksum1 := binary.BigEndian.Uint32(frame[16:])
		fchksum2 := binary.BigEndian.Uint32(frame[20:])
		chksum1, chksum2 = WALChecksum(db.wal.byteOrder, chksum1, chksum2, frame[:8])  // frame header
		chksum1, chksum2 = WALChecksum(db.wal.byteOrder, chksum1, chksum2, frame[24:]) // frame data
		if chksum1 != fchksum1 || chksum2 != fchksum2 {
			return nil, 0, 0, 0, 0, errNoTransaction
		}

		// Save the offset for the last version of the page to a map.
		pgno := binary.BigEndian.Uint32(frame[0:])
		m[pgno] = offset

		// End of transaction, exit loop and return.
		if commit = binary.BigEndian.Uint32(frame[4:]); commit != 0 {
			return m, commit, chksum1, chksum2, offset + int64(len(frame)), nil
		}

		// Move to the next frame.
		offset += int64(len(frame))
	}
}

// errNoTransaction is a marker error to indicate that a transaction could not
// be found at the current WAL offset.
var errNoTransaction = errors.New("no transaction")

// commitWAL is called when the client releases the WAL_WRITE_LOCK(120).
// The transaction data is copied from the WAL into an LTX file and committed.
func (db *DB) CommitWAL() error {
	walFrameSize := int64(WALFrameHeaderSize + db.pageSize)

	walFile, err := os.Open(db.WALPath())
	if err != nil {
		return fmt.Errorf("open wal file: %w", err)
	}
	defer func() { _ = walFile.Close() }()

	// Sync WAL to disk as this avoids data loss issues with SYNCHRONOUS=normal
	if err := walFile.Sync(); err != nil {
		return fmt.Errorf("sync wal: %w", err)
	}

	// Build offset map for the last version of each page in the WAL transaction.
	// If txFrameOffsets has no entries then a transaction could not be found after db.wal.offset.
	txFrameOffsets, commit, chksum1, chksum2, endOffset, err := db.buildTxFrameOffsets(walFile)
	if err == errNoTransaction {
		return nil
	} else if err != nil {
		return fmt.Errorf("build tx frame offsets: %w", err)
	}

	dbFile, err := os.Open(db.DatabasePath())
	if err != nil {
		return fmt.Errorf("cannot open database file: %w", err)
	}
	defer func() { _ = dbFile.Close() }()

	// Read previous page 1 and extract page count.
	page := make([]byte, db.pageSize)
	if err := db.readPage(dbFile, walFile, 1, page); err != nil {
		return fmt.Errorf("read prev page 1: %w", err)
	}
	prevPageN := binary.BigEndian.Uint32(page[SQLITE_DATABASE_SIZE_OFFSET:])

	// Determine transaction ID of the in-process transaction.
	pos := db.Pos()
	txID := pos.TXID + 1

	// Compute rolling checksum based off previous LTX database checksum.
	preApplyChecksum := pos.PostApplyChecksum
	postApplyChecksum := pos.PostApplyChecksum // start from previous chksum

	// Open file descriptors for the header & page blocks for new LTX file.
	ltxPath := db.LTXPath(txID, txID)
	tmpPath := ltxPath + ".tmp"
	_ = os.Remove(tmpPath)

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create LTX file: %w", err)
	}
	defer func() { _ = f.Close() }()

	enc := ltx.NewEncoder(f)
	if err := enc.EncodeHeader(ltx.Header{
		Version:          1,
		PageSize:         db.pageSize,
		Commit:           commit,
		MinTXID:          txID,
		MaxTXID:          txID,
		Timestamp:        db.Now().UnixMilli(),
		PreApplyChecksum: preApplyChecksum,
		WALSalt1:         db.wal.salt1,
		WALSalt2:         db.wal.salt2,
		WALOffset:        db.wal.offset,
		WALSize:          endOffset - db.wal.offset,
	}); err != nil {
		return fmt.Errorf("cannot encode ltx header: %s", err)
	}

	// Build sorted list of page numbers in current transaction.
	pgnos := make([]uint32, 0, len(txFrameOffsets))
	for pgno := range txFrameOffsets {
		pgnos = append(pgnos, pgno)
	}
	sort.Slice(pgnos, func(i, j int) bool { return pgnos[i] < pgnos[j] })

	frame := make([]byte, walFrameSize)
	for _, pgno := range pgnos {
		// Read next frame from the WAL file.
		offset := txFrameOffsets[pgno]
		if _, err := internal.ReadFullAt(walFile, frame, offset); err != nil {
			return fmt.Errorf("read next frame: %w", err)
		}
		pgno := binary.BigEndian.Uint32(frame[0:4])

		// Copy page into LTX file.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, frame[WALFrameHeaderSize:]); err != nil {
			return fmt.Errorf("cannot encode ltx page: pgno=%d err=%w", pgno, err)
		}

		// Update rolling checksum.
		postApplyChecksum ^= ltx.ChecksumPage(pgno, frame[WALFrameHeaderSize:])
	}

	// Add truncated pages to page numbers so they can be removed.
	for pgno := commit + 1; pgno <= prevPageN; pgno++ {
		pgnos = append(pgnos, pgno)
	}

	// Remove checksums from old pages, if they existed.
	// This can be found either earlier in the WAL or from the database.
	for _, pgno := range pgnos {
		if pgno > prevPageN {
			continue
		}
		if err := db.readPage(dbFile, walFile, pgno, page); err != nil {
			return fmt.Errorf("read page: pgno=%d err=%w", pgno, err)
		}

		postApplyChecksum ^= ltx.ChecksumPage(pgno, page)
	}

	// Ensure checksum flag is applied.
	postApplyChecksum |= ltx.ChecksumFlag

	// Finish page block to compute checksum and then finish header block.
	enc.SetPostApplyChecksum(postApplyChecksum)
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close ltx encoder: %s", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("sync ltx file: %s", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("close ltx file: %s", err)
	}

	// Atomically rename the file
	if err := os.Rename(tmpPath, ltxPath); err != nil {
		return fmt.Errorf("rename ltx file: %w", err)
	} else if err := internal.Sync(filepath.Dir(ltxPath)); err != nil {
		return fmt.Errorf("sync ltx dir: %w", err)
	}

	// Copy page offsets on commit.
	for pgno, off := range txFrameOffsets {
		db.wal.frameOffsets[pgno] = off
	}

	// Move the WAL position forward and reset the segment size.
	db.pageN = commit
	db.wal.offset = endOffset
	db.wal.chksum1 = chksum1
	db.wal.chksum2 = chksum2

	// Update transaction for database.
	if err := db.setPos(Pos{
		TXID:              enc.Header().MaxTXID,
		PostApplyChecksum: enc.Trailer().PostApplyChecksum,
	}); err != nil {
		return fmt.Errorf("set pos: %w", err)
	}

	// Update metrics
	dbCommitCountMetricVec.WithLabelValues(db.name).Inc()
	dbLTXCountMetricVec.WithLabelValues(db.name).Inc()
	dbLTXBytesMetricVec.WithLabelValues(db.name).Set(float64(enc.N()))
	dbLatencySecondsMetricVec.WithLabelValues(db.name).Set(0.0)

	// Notify store of database change.
	db.store.MarkDirty(db.name)

	// Perform full checksum verification, if set. For testing only.
	if db.store.StrictVerify {
		if chksum, err := db.checksum(dbFile, walFile); err != nil {
			return fmt.Errorf("checksum (wal): %w", err)
		} else if chksum != postApplyChecksum {

			// TODO: Remove!!
			fmt.Printf("verification failed (wal): %016x <> %016x\n", chksum, postApplyChecksum)
			os.Exit(1)

			return fmt.Errorf("verification failed (wal): %016x <> %016x", chksum, postApplyChecksum)
		}
	}

	return nil
}

// readPage reads the latest version of the page before the current transaction.
func (db *DB) readPage(dbFile, walFile *os.File, pgno uint32, buf []byte) error {
	// Read from previous position in WAL, if available.
	if off, ok := db.wal.frameOffsets[pgno]; ok {
		hdr := make([]byte, WALFrameHeaderSize)
		if _, err := internal.ReadFullAt(walFile, hdr, off); err != nil {
			return fmt.Errorf("read wal page hdr: %w", err)
		}

		if _, err := internal.ReadFullAt(walFile, buf, off+WALFrameHeaderSize); err != nil {
			return fmt.Errorf("read wal page: %w", err)
		}
		return nil
	}

	// Otherwise read from the database file.
	offset := int64(pgno-1) * int64(db.pageSize)
	if _, err := internal.ReadFullAt(dbFile, buf, offset); err != nil {
		return fmt.Errorf("read database page: %w", err)
	}
	return nil
}

// CreateSHM creates a new shared memory file on disk.
func (db *DB) CreateSHM() (*os.File, error) {
	f, err := os.OpenFile(db.SHMPath(), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
	TraceLog.Printf("[CreateSHM(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// OpenSHM returns a handle for the shared memory file.
func (db *DB) OpenSHM(ctx context.Context) (*os.File, error) {
	f, err := os.OpenFile(db.SHMPath(), os.O_RDWR, 0666)
	TraceLog.Printf("[OpenSHM(%s)]: %s", db.name, errorKeyValue(err))
	return f, err
}

// CloseSHM closes a handle associated with the SHM file.
func (db *DB) CloseSHM(ctx context.Context, f *os.File, owner uint64) error {
	err := f.Close()
	TraceLog.Printf("[CloseSHM(%s)]: owner=%d %s", db.name, owner, errorKeyValue(err))
	return err
}

// SyncSHM fsync's the shared memory file.
func (db *DB) SyncSHM(ctx context.Context) (err error) {
	defer func() {
		TraceLog.Printf("[SyncSHM(%s)]: %s", db.name, errorKeyValue(err))
	}()

	f, err := os.Open(db.SHMPath())
	if err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	return f.Close()
}

// TruncateSHM sets the size of the the SHM file.
func (db *DB) TruncateSHM(ctx context.Context, size int64) error {
	err := os.Truncate(db.SHMPath(), size)
	TraceLog.Printf("[TruncateSHM(%s)]: size=%d %s", db.name, size, errorKeyValue(err))
	return err
}

// RemoveSHM removes the SHM file from disk.
func (db *DB) RemoveSHM(ctx context.Context) error {
	err := os.Remove(db.SHMPath())
	TraceLog.Printf("[RemoveSHM(%s)]: %s", db.name, errorKeyValue(err))
	return err
}

// UnlockSHM unlocks all locks from the SHM file.
func (db *DB) UnlockSHM(ctx context.Context, owner uint64) {
	guardSet := db.GuardSet(owner)
	if guardSet == nil {
		return
	}
	guardSet.UnlockSHM()
	TraceLog.Printf("[UnlockSHM(%s)]: owner=%d", db.name, owner)
}

// ReadSHMAt reads from the shared memory at the specified offset.
func (db *DB) ReadSHMAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (int, error) {
	n, err := f.ReadAt(data, offset)
	TraceLog.Printf("[ReadSHMAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
	return n, err
}

// WriteSHMAt writes data to the SHM file.
func (db *DB) WriteSHMAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (int, error) {
	dbSHMWriteCountMetricVec.WithLabelValues(db.name).Inc()
	n, err := f.WriteAt(data, offset)
	TraceLog.Printf("[WriteSHMAt(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
	return n, err
}

// isByteSliceZero returns true if b only contains NULL bytes.
func isByteSliceZero(b []byte) bool {
	for _, v := range b {
		if v != 0 {
			return false
		}
	}
	return true
}

// CommitJournal deletes the journal file which commits or rolls back the transaction.
func (db *DB) CommitJournal(mode JournalMode) error {
	// Return an error if the current process is not the leader.
	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	}

	// Read journal header to ensure it's valid.
	if ok, err := db.isJournalHeaderValid(); err != nil {
		return err
	} else if !ok {
		return db.invalidateJournal(mode) // rollback
	}

	// If there is no page size available then nothing has been written.
	// Continue with the invalidation without processing the journal.
	if db.pageSize == 0 {
		if err := db.invalidateJournal(mode); err != nil {
			return fmt.Errorf("invalidate journal: %w", err)
		}
		return nil
	}

	// Determine transaction ID of the in-process transaction.
	pos := db.Pos()
	txID := pos.TXID + 1
	prevPageN := db.pageN

	dbFile, err := os.Open(db.DatabasePath())
	if err != nil {
		return fmt.Errorf("cannot open database file: %w", err)
	}
	defer func() { _ = dbFile.Close() }()

	var commit uint32
	if _, err := dbFile.Seek(SQLITE_DATABASE_SIZE_OFFSET, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to database size: %w", err)
	} else if err := binary.Read(dbFile, binary.BigEndian, &commit); err != nil {
		return fmt.Errorf("cannot read database size: %w", err)
	}

	// Compute rolling checksum based off previous LTX database checksum.
	preApplyChecksum := pos.PostApplyChecksum
	postApplyChecksum := pos.PostApplyChecksum // start from previous chksum

	// Remove page checksums from old pages in the journal.
	journalFile, err := os.Open(db.JournalPath())
	if err != nil {
		return fmt.Errorf("cannot open journal file: %w", err)
	}

	journalPageMap, err := buildJournalPageMap(journalFile, db.pageSize)
	if err != nil {
		return fmt.Errorf("cannot build journal page map: %w", err)
	}

	for _, pageChksum := range journalPageMap {
		postApplyChecksum ^= pageChksum
	}

	// Build sorted list of dirty page numbers.
	pgnos := make([]uint32, 0, len(db.dirtyPageSet))
	for pgno := range db.dirtyPageSet {
		if pgno <= commit {
			pgnos = append(pgnos, pgno)
		}
	}
	sort.Slice(pgnos, func(i, j int) bool { return pgnos[i] < pgnos[j] })

	// Open file descriptors for the header & page blocks for new LTX file.
	ltxPath := db.LTXPath(txID, txID)
	tmpPath := ltxPath + ".tmp"
	_ = os.Remove(tmpPath)

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("cannot create LTX file: %w", err)
	}
	defer func() { _ = f.Close() }()

	enc := ltx.NewEncoder(f)
	if err := enc.EncodeHeader(ltx.Header{
		Version:          1,
		PageSize:         db.pageSize,
		Commit:           commit,
		MinTXID:          txID,
		MaxTXID:          txID,
		Timestamp:        db.Now().UnixMilli(),
		PreApplyChecksum: preApplyChecksum,
	}); err != nil {
		return fmt.Errorf("cannot encode ltx header: %s", err)
	}

	// Copy transactions from main database to the LTX file in sorted order.
	buf := make([]byte, db.pageSize)
	dbMode := DBModeRollback
	for _, pgno := range pgnos {
		// Read page from database.
		offset := int64(pgno-1) * int64(db.pageSize)
		if _, err := internal.ReadFullAt(dbFile, buf, offset); err != nil {
			return fmt.Errorf("cannot read database page: pgno=%d err=%w", pgno, err)
		}

		// Copy page into LTX file.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return fmt.Errorf("cannot encode ltx page: pgno=%d err=%w", pgno, err)
		}

		// Update the mode if this is the first page and the write/read versions as set to WAL (2).
		if pgno == 1 && buf[18] == 2 && buf[19] == 2 {
			dbMode = DBModeWAL
		}

		// Verify updated page matches in-memory checksum.
		pageChksum := db.chksums[pgno]
		bufChksum := ltx.ChecksumPage(pgno, buf)
		if bufChksum != pageChksum {
			return fmt.Errorf("updated page (%d) does not match in-memory checksum: %016x <> %016x (%016x)", pgno, bufChksum, pageChksum, bufChksum^pageChksum)
		}

		// Update rolling checksum.
		postApplyChecksum ^= bufChksum
	}

	// Checksum pages removed by truncation.
	for pgno := commit + 1; pgno <= prevPageN; pgno++ {
		offset := (int64(pgno) - 1) * int64(db.pageSize)
		if _, err := internal.ReadFullAt(dbFile, buf, offset); err != nil {
			return fmt.Errorf("cannot read truncated database page: pgno=%d err=%w", pgno, err)
		}

		// Verify checksum matches in-memory checksum.
		pageChksum := db.chksums[pgno]
		bufChksum := ltx.ChecksumPage(pgno, buf)
		if bufChksum != pageChksum {
			return fmt.Errorf("truncated page %d does not match in-memory checksum: %016x <> %016x (%016x)", pgno, bufChksum, pageChksum, bufChksum^pageChksum)
		}

		// Remove from checksum on position.
		postApplyChecksum ^= bufChksum
	}

	// Ensure checksum flag is applied.
	postApplyChecksum |= ltx.ChecksumFlag

	// Finish page block to compute checksum and then finish header block.
	enc.SetPostApplyChecksum(postApplyChecksum)
	if err := enc.Close(); err != nil {
		return fmt.Errorf("close ltx encoder: %s", err)
	} else if err := f.Sync(); err != nil {
		return fmt.Errorf("sync ltx file: %s", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("close ltx file: %s", err)
	}

	// Atomically rename the file
	if err := os.Rename(tmpPath, ltxPath); err != nil {
		return fmt.Errorf("rename ltx file: %w", err)
	} else if err := internal.Sync(filepath.Dir(ltxPath)); err != nil {
		return fmt.Errorf("sync ltx dir: %w", err)
	}

	// Ensure file is persisted to disk.
	if err := dbFile.Sync(); err != nil {
		return fmt.Errorf("cannot sync ltx file: %w", err)
	}

	if err := db.invalidateJournal(mode); err != nil {
		return fmt.Errorf("invalidate journal: %w", err)
	}

	// Update database flags.
	db.pageN = commit
	db.mode = dbMode

	// Update transaction for database.
	if err := db.setPos(Pos{
		TXID:              enc.Header().MaxTXID,
		PostApplyChecksum: enc.Trailer().PostApplyChecksum,
	}); err != nil {
		return fmt.Errorf("set pos: %w", err)
	}

	// Update metrics
	dbCommitCountMetricVec.WithLabelValues(db.name).Inc()
	dbLTXCountMetricVec.WithLabelValues(db.name).Inc()
	dbLTXBytesMetricVec.WithLabelValues(db.name).Set(float64(enc.N()))
	dbLatencySecondsMetricVec.WithLabelValues(db.name).Set(0.0)

	// Notify store of database change.
	db.store.MarkDirty(db.name)

	// Calculate checksum for entire database.
	if db.store.StrictVerify {
		if chksum, err := db.checksum(dbFile, nil); err != nil {
			return fmt.Errorf("checksum (journal): %w", err)
		} else if chksum != postApplyChecksum {
			return fmt.Errorf("verification failed (journal): %016x <> %016x", chksum, postApplyChecksum)
		}
	}

	return nil
}

// checksum calculates the LTX checksum of the current state of the database.
func (db *DB) checksum(dbFile, walFile *os.File) (chksum uint64, err error) {
	if db.pageSize == 0 {
		return 0, fmt.Errorf("page size required for checksum")
	} else if db.pageN == 0 {
		return 0, fmt.Errorf("page count required for checksum")
	}

	data := make([]byte, db.pageSize)
	for pgno := uint32(1); pgno <= db.pageN; pgno++ {
		// Read from either the database file or the WAL depending if the page exists in the WAL.
		if offset, ok := db.wal.frameOffsets[pgno]; !ok {
			if _, err := dbFile.Seek(int64(pgno-1)*int64(db.pageSize), io.SeekStart); err != nil {
				return 0, fmt.Errorf("db seek (pgno=%d): %w", pgno, err)
			} else if _, err := io.ReadFull(dbFile, data); err != nil {
				return 0, fmt.Errorf("db read (pgno=%d): %w", pgno, err)
			}
		} else {
			if _, err := walFile.Seek(offset+WALFrameHeaderSize, io.SeekStart); err != nil {
				return 0, fmt.Errorf("wal seek (pgno=%d, wal-offset=%d): %w", pgno, offset, err)
			} else if _, err := io.ReadFull(walFile, data); err != nil {
				return 0, fmt.Errorf("wal read (pgno=%d): %w", pgno, err)
			}
		}

		// Add the page to the rolling checksum.
		chksum ^= ltx.ChecksumPage(pgno, data)
	}
	return ltx.ChecksumFlag | chksum, nil
}

// isJournalHeaderValid returns true if the journal starts with the journal magic.
func (db *DB) isJournalHeaderValid() (bool, error) {
	f, err := os.Open(db.JournalPath())
	if err != nil {
		return false, err
	}
	defer func() { _ = f.Close() }()

	buf := make([]byte, len(SQLITE_JOURNAL_HEADER_STRING))
	if _, err := io.ReadFull(f, buf); err != nil {
		return false, err
	}
	return string(buf) == SQLITE_JOURNAL_HEADER_STRING, nil
}

// invalidateJournal invalidates the journal file based on the journal mode.
func (db *DB) invalidateJournal(mode JournalMode) error {
	switch mode {
	case JournalModeDelete:
		if err := os.Remove(db.JournalPath()); err != nil {
			return fmt.Errorf("remove journal file: %w", err)
		}

	case JournalModeTruncate:
		if err := os.Truncate(db.JournalPath(), 0); err != nil {
			return fmt.Errorf("truncate: %w", err)
		} else if err := internal.Sync(db.JournalPath()); err != nil {
			return fmt.Errorf("sync journal: %w", err)
		}

	case JournalModePersist:
		f, err := os.OpenFile(db.JournalPath(), os.O_RDWR, 0666)
		if err != nil && !os.IsNotExist(err) {
			return fmt.Errorf("open journal: %w", err)
		} else if err == nil {
			defer func() { _ = f.Close() }()

			if _, err := f.Write(make([]byte, SQLITE_JOURNAL_HEADER_SIZE)); err != nil {
				return fmt.Errorf("clear journal header: %w", err)
			} else if err := f.Sync(); err != nil {
				return fmt.Errorf("sync journal: %w", err)
			}
		}

	default:
		return fmt.Errorf("invalid journal: %q", mode)
	}

	// Sync the underlying directory.
	if err := internal.Sync(db.path); err != nil {
		return fmt.Errorf("sync database directory: %w", err)
	}

	db.dirtyPageSet = make(map[uint32]struct{})

	return nil
}

// ApplyLTX applies an LTX file to the database.
func (db *DB) ApplyLTX(ctx context.Context, path string) error {
	guard, err := db.AcquireWriteLock(ctx)
	if err != nil {
		return err
	}
	defer guard.Unlock()

	return db.applyLTX(ctx, path)
}

func (db *DB) applyLTX(ctx context.Context, path string) error {
	// Open database file for writing.
	dbf, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open database file: %w", err)
	}
	defer func() { _ = dbf.Close() }()

	// Open LTX header reader.
	hf, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer func() { _ = hf.Close() }()

	dec := ltx.NewDecoder(hf)
	if err := dec.DecodeHeader(); err != nil {
		return fmt.Errorf("decode ltx header: %s", err)
	}

	dbMode := db.mode
	pageBuf := make([]byte, dec.Header().PageSize)
	for i := 0; ; i++ {
		// Read pgno & page data from LTX file.
		var phdr ltx.PageHeader
		if err := dec.DecodePage(&phdr, pageBuf); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode ltx page[%d]: %w", i, err)
		}

		// Update the mode if this is the first page and the write/read versions as set to WAL (2).
		if phdr.Pgno == 1 && pageBuf[18] == 2 && pageBuf[19] == 2 {
			dbMode = DBModeWAL
		}

		// Copy to database file.
		offset := int64(phdr.Pgno-1) * int64(dec.Header().PageSize)
		if _, err := dbf.WriteAt(pageBuf, offset); err != nil {
			return fmt.Errorf("write to database file: %w", err)
		}

		// Invalidate page cache.
		if invalidator := db.store.Invalidator; invalidator != nil {
			if err := invalidator.InvalidateDBRange(db, offset, int64(len(pageBuf))); err != nil {
				return fmt.Errorf("invalidate db: %w", err)
			}
		}
	}

	// Close the reader so we can verify file integrity.
	if err := dec.Close(); err != nil {
		return fmt.Errorf("close ltx decode: %w", err)
	}

	// Truncate database file to size after LTX file.
	if err := dbf.Truncate(int64(dec.Header().Commit) * int64(dec.Header().PageSize)); err != nil {
		return fmt.Errorf("truncate database file: %w", err)
	}

	// Sync changes to disk.
	if err := dbf.Sync(); err != nil {
		return fmt.Errorf("sync database file: %w", err)
	}

	db.mode = dbMode

	if db.pageSize == 0 {
		db.pageSize = dec.Header().PageSize
	}
	db.pageN = dec.Header().Commit

	// Update transaction for database.
	if err := db.setPos(Pos{
		TXID:              dec.Header().MaxTXID,
		PostApplyChecksum: dec.Trailer().PostApplyChecksum,
	}); err != nil {
		return fmt.Errorf("set pos: %w", err)
	}

	// Invalidate SHM so that the transaction is visible.
	if err := db.invalidateSHM(ctx); err != nil {
		return fmt.Errorf("invalidate shm: %w", err)
	}

	// Notify store of database change.
	db.store.MarkDirty(db.name)

	// Calculate latency since LTX file was written.
	latency := float64(time.Now().UnixMilli()-dec.Header().Timestamp) / 1000
	dbLatencySecondsMetricVec.WithLabelValues(db.name).Set(latency)

	return nil
}

// invalidateSHM clears the SHM header so that SQLite needs to rebuild it.
func (db *DB) invalidateSHM(ctx context.Context) error {
	f, err := os.OpenFile(db.SHMPath(), os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		return nil // no shm, exit
	} else if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Invalidate page cache.
	if invalidator := db.store.Invalidator; invalidator != nil {
		if err := invalidator.InvalidateSHM(db); err != nil {
			return err
		}
	}

	// Clear the SHM file header.
	if _, err := f.Write(make([]byte, 135)); err != nil {
		return err
	}

	return nil
}

// Import replaces the contents of the database with the contents from the r.
// NOTE: LiteFS does not validate the integrity of the imported database!
func (db *DB) Import(ctx context.Context, r io.Reader) error {
	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	}

	// Acquire write lock.
	guard, err := db.AcquireWriteLock(ctx)
	if err != nil {
		return err
	}
	defer guard.Unlock()

	pos, err := db.importToLTX(ctx, r)
	if err != nil {
		return err
	}

	// Invalidate journal, if one exists.
	if err := db.invalidateJournal(JournalModePersist); err != nil {
		return fmt.Errorf("invalidate journal: %w", err)
	}

	// Truncate WAL, if it exists.
	if _, err := os.Stat(db.WALPath()); err == nil {
		if err := os.Truncate(db.WALPath(), 0); err != nil {
			return fmt.Errorf("truncate wal: %w", err)
		}
	}

	return db.applyLTX(ctx, db.LTXPath(pos.TXID, pos.TXID))
}

// importToLTX reads a SQLite database and writes it to the next LTX file.
func (db *DB) importToLTX(ctx context.Context, r io.Reader) (Pos, error) {
	// Read header to determine DB mode, page size, & commit.
	hdr, data, err := readSQLiteDatabaseHeader(r)
	if err != nil {
		return Pos{}, fmt.Errorf("read database header: %w", err)
	}

	// Prepend header back onto original reader.
	r = io.MultiReader(bytes.NewReader(data), r)

	// Determine resulting position.
	pos := db.Pos()
	pos.TXID++
	preApplyChecksum := pos.PostApplyChecksum
	pos.PostApplyChecksum = 0

	// Open file descriptors for the header & page blocks for new LTX file.
	ltxPath := db.LTXPath(pos.TXID, pos.TXID)
	tmpPath := ltxPath + ".tmp"
	_ = os.Remove(tmpPath)

	f, err := os.Create(tmpPath)
	if err != nil {
		return Pos{}, fmt.Errorf("cannot create LTX file: %w", err)
	}
	defer func() { _ = f.Close() }()

	enc := ltx.NewEncoder(f)
	if err := enc.EncodeHeader(ltx.Header{
		Version:          1,
		PageSize:         hdr.PageSize,
		Commit:           hdr.PageN,
		MinTXID:          pos.TXID,
		MaxTXID:          pos.TXID,
		Timestamp:        db.Now().UnixMilli(),
		PreApplyChecksum: preApplyChecksum,
	}); err != nil {
		return Pos{}, fmt.Errorf("cannot encode ltx header: %s", err)
	}

	// Generate LTX file from reader.
	buf := make([]byte, hdr.PageSize)
	for pgno := uint32(1); pgno <= hdr.PageN; pgno++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return Pos{}, fmt.Errorf("read page %d: %w", pgno, err)
		}

		// Reset file change counter & schema cookie to ensure existing connections reload.
		if pgno == 1 {
			binary.BigEndian.PutUint32(buf[24:], 0)
			binary.BigEndian.PutUint32(buf[40:], 0)
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return Pos{}, fmt.Errorf("encode ltx page: pgno=%d err=%w", pgno, err)
		}

		pos.PostApplyChecksum ^= ltx.ChecksumPage(pgno, buf)
	}

	pos.PostApplyChecksum |= ltx.ChecksumFlag

	// Finish page block to compute checksum and then finish header block.
	enc.SetPostApplyChecksum(pos.PostApplyChecksum)
	if err := enc.Close(); err != nil {
		return Pos{}, fmt.Errorf("close ltx encoder: %s", err)
	} else if err := f.Sync(); err != nil {
		return Pos{}, fmt.Errorf("sync ltx file: %s", err)
	} else if err := f.Close(); err != nil {
		return Pos{}, fmt.Errorf("close ltx file: %s", err)
	}

	// Atomically rename the file
	if err := os.Rename(tmpPath, ltxPath); err != nil {
		return Pos{}, fmt.Errorf("rename ltx file: %w", err)
	} else if err := internal.Sync(filepath.Dir(ltxPath)); err != nil {
		return Pos{}, fmt.Errorf("sync ltx dir: %w", err)
	}

	return pos, nil
}

// AcquireWriteLock acquires the appropriate locks for a write depending on if
// the database uses a rollback journal or WAL.
func (db *DB) AcquireWriteLock(ctx context.Context) (_ *GuardSet, err error) {
	gs := db.newGuardSet(0) // TODO(fsm): Track internal owners?
	defer func() {
		if err != nil {
			gs.Unlock()
		}
	}()

	// Acquire shared lock to check database mode.
	if err := gs.pending.RLock(ctx); err != nil {
		return nil, fmt.Errorf("acquire PENDING read lock: %w", err)
	}
	if err := gs.shared.RLock(ctx); err != nil {
		return nil, fmt.Errorf("acquire SHARED read lock: %w", err)
	}
	gs.pending.Unlock()

	// If this is a rollback journal, upgrade all database locks to exclusive.
	if db.mode == DBModeRollback {
		if err := gs.reserved.Lock(ctx); err != nil {
			return nil, fmt.Errorf("acquire RESERVED write lock: %w", err)
		}
		if err := gs.pending.Lock(ctx); err != nil {
			return nil, fmt.Errorf("acquire PENDING write lock: %w", err)
		}
		if err := gs.shared.Lock(ctx); err != nil {
			return nil, fmt.Errorf("acquire SHARED write lock: %w", err)
		}
		return gs, nil
	}

	if err := gs.write.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_WRITE_LOCK: %w", err)
	}
	if err := gs.ckpt.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_CKPT_LOCK: %w", err)
	}
	if err := gs.recover.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_RECOVER_LOCK: %w", err)
	}
	if err := gs.read0.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_READ0_LOCK: %w", err)
	}
	if err := gs.read1.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_READ1_LOCK: %w", err)
	}
	if err := gs.read2.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_READ2_LOCK: %w", err)
	}
	if err := gs.read3.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_READ3_LOCK: %w", err)
	}
	if err := gs.read4.Lock(ctx); err != nil {
		return nil, fmt.Errorf("acquire exclusive WAL_READ4_LOCK: %w", err)
	}

	return gs, nil
}

// GuardSet returns a guard set for the given owner, if it exists.
func (db *DB) GuardSet(owner uint64) *GuardSet {
	db.guardSets.mu.Lock()
	defer db.guardSets.mu.Unlock()
	return db.guardSets.m[owner]
}

// CreateGuardSetIfNotExists returns a guard set for the given owner.
// Creates a new guard set if one is not associated with the owner.
func (db *DB) CreateGuardSetIfNotExists(owner uint64) *GuardSet {
	db.guardSets.mu.Lock()
	defer db.guardSets.mu.Unlock()

	guardSet := db.guardSets.m[owner]
	if guardSet == nil {
		guardSet = db.newGuardSet(owner)
		db.guardSets.m[owner] = guardSet
	}
	return guardSet
}

// newGuardSet returns a set of guards that can control locking for the database file.
func (db *DB) newGuardSet(owner uint64) *GuardSet {
	return &GuardSet{
		owner: owner,

		pending:  db.pendingLock.Guard(),
		shared:   db.sharedLock.Guard(),
		reserved: db.reservedLock.Guard(),

		write:   db.writeLock.Guard(),
		ckpt:    db.ckptLock.Guard(),
		recover: db.recoverLock.Guard(),
		read0:   db.read0Lock.Guard(),
		read1:   db.read1Lock.Guard(),
		read2:   db.read2Lock.Guard(),
		read3:   db.read3Lock.Guard(),
		read4:   db.read4Lock.Guard(),
		dms:     db.dmsLock.Guard(),
	}
}

// TryLocks attempts to lock one or more locks on the database for a given owner.
// Returns an error if no locks are supplied.
func (db *DB) TryLocks(ctx context.Context, owner uint64, lockTypes []LockType) (bool, error) {
	guardSet := db.CreateGuardSetIfNotExists(owner)
	for _, lockType := range lockTypes {
		guard := guardSet.Guard(lockType)
		ok := guard.TryLock()

		status := "OK"
		if !ok {
			status = "FAIL"
		}
		TraceLog.Printf("[TryLock(%s)]: type=%s owner=%d status=%s", db.name, lockType, owner, status)

		if !ok {
			return false, nil
		}

		// Handle locks that signal the start of a transaction.
		switch lockType {
		case LockTypeReserved:
			if err := db.beginJournalWriteTx(ctx, owner); err != nil {
				guard.Unlock()
				return false, err
			}
		}
	}
	return true, nil
}

// CanLock returns true if all locks can acquire a write lock.
// If false, also returns the mutex state of the blocking lock.
func (db *DB) CanLock(ctx context.Context, owner uint64, lockTypes []LockType) (bool, RWMutexState) {
	guardSet := db.CreateGuardSetIfNotExists(owner)
	for _, lockType := range lockTypes {
		guard := guardSet.Guard(lockType)
		if canLock, mutexState := guard.CanLock(); !canLock {
			return false, mutexState
		}
	}
	return true, RWMutexStateUnlocked
}

// TryRLocks attempts to read lock one or more locks on the database for a given owner.
// Returns an error if no locks are supplied.
func (db *DB) TryRLocks(ctx context.Context, owner uint64, lockTypes []LockType) bool {
	guardSet := db.CreateGuardSetIfNotExists(owner)
	for _, lockType := range lockTypes {
		ok := guardSet.Guard(lockType).TryRLock()

		status := "OK"
		if !ok {
			status = "FAIL"
		}
		TraceLog.Printf("[TryRLock(%s)]: type=%s owner=%d status=%s", db.name, lockType, owner, status)

		if !ok {
			return false
		}
	}
	return true
}

// CanRLock returns true if all locks can acquire a read lock.
func (db *DB) CanRLock(ctx context.Context, owner uint64, lockTypes []LockType) bool {
	guardSet := db.CreateGuardSetIfNotExists(owner)
	for _, lockType := range lockTypes {
		if !guardSet.Guard(lockType).CanRLock() {
			return false
		}
	}
	return true
}

// Unlock unlocks one or more locks on the database for a given owner.
func (db *DB) Unlock(ctx context.Context, owner uint64, lockTypes []LockType) {
	guardSet := db.GuardSet(owner)
	if guardSet == nil {
		return
	}

	// Process WAL if we have an exclusive lock on WAL_WRITE_LOCK.
	if ContainsLockType(lockTypes, LockTypeWrite) && guardSet.Write().State() == RWMutexStateExclusive {
		if err := db.CommitWAL(); err != nil {
			log.Printf("commit wal error: %s", err)
		}
	}

	for _, lockType := range lockTypes {
		TraceLog.Printf("[Unlock(%s)]: type=%s owner=%d", db.name, lockType, owner)
		guardSet.Guard(lockType).Unlock()
	}

	// TODO: Release guard set if completely unlocked.
}

// InWriteTx returns true if the RESERVED lock has an exclusive lock.
func (db *DB) InWriteTx() bool {
	return db.reservedLock.State() == RWMutexStateExclusive
}

// beginJournalWriteTx is called when an exclusive RESERVED lock is obtained.
func (db *DB) beginJournalWriteTx(ctx context.Context, owner uint64) error {
	if err := db.verifyChecksums(ctx); err != nil {
		return fmt.Errorf("pre-transaction checksum validation: %w", err)
	}
	return nil
}

// endJournalWriteTx is called when an exclusive RESERVED lock is released.
func (db *DB) endJournalWriteTx(ctx context.Context, owner uint64) error {
	if err := db.verifyChecksums(ctx); err != nil {
		return fmt.Errorf("post-transaction checksum validation failed: %w", err)
	}
	return nil
}

// verifyChecksums computes the checksum from the in-memory page checksums and
// verifies that it matches the checksum in the position.
func (db *DB) verifyChecksums(ctx context.Context) error {
	var chksum uint64
	for _, pageChksum := range db.chksums {
		chksum = ltx.ChecksumFlag | (chksum ^ pageChksum)
	}

	if pos := db.Pos(); chksum != pos.PostApplyChecksum {
		return fmt.Errorf("in-memory checksum mismatch: %s <> %016x", pos, chksum)
	}
	return nil
}

// WriteSnapshotTo writes an LTX snapshot to dst.
func (db *DB) WriteSnapshotTo(ctx context.Context, dst io.Writer) (header ltx.Header, trailer ltx.Trailer, err error) {
	gs := db.newGuardSet(0) // TODO(fsm): Track internal owners?
	defer gs.Unlock()

	// Acquire PENDING then SHARED. Release PENDING immediately afterward.
	if err := gs.pending.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire PENDING read lock: %w", err)
	}
	if err := gs.shared.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire SHARED read lock: %w", err)
	}
	gs.pending.Unlock()

	// If this is WAL mode then temporarily obtain a write lock so we can copy
	// out the current database size & wal frames before returning to a read lock.
	if db.mode == DBModeWAL {
		if err := gs.write.Lock(ctx); err != nil {
			return header, trailer, fmt.Errorf("acquire temporary exclusive WAL_WRITE_LOCK: %w", err)
		}
	}

	// Determine current position & snapshot overriding WAL frames.
	pos := db.Pos()
	pageSize, pageN := db.pageSize, db.pageN
	walFrameOffsets := make(map[uint32]int64, len(db.wal.frameOffsets))
	for k, v := range db.wal.frameOffsets {
		walFrameOffsets[k] = v
	}

	// Release write lock, if acquired.
	gs.write.Unlock()

	// Acquire the CKPT & READ locks to prevent checkpointing, in case this is in WAL mode.
	if err := gs.read0.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire READ0 read lock: %w", err)
	}
	if err := gs.read1.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire READ1 read lock: %w", err)
	}
	if err := gs.read2.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire READ2 read lock: %w", err)
	}
	if err := gs.read3.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire READ3 read lock: %w", err)
	}
	if err := gs.read4.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire READ4 read lock: %w", err)
	}
	if err := gs.ckpt.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire CKPT read lock: %w", err)
	}
	if err := gs.recover.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire RECOVER read lock: %w", err)
	}

	// Log transaction ID for the snapshot.
	log.Printf("writing snapshot %q @ %s", db.name, ltx.FormatTXID(pos.TXID))

	// Open database file.
	dbFile, err := os.Open(db.DatabasePath())
	if err != nil {
		return header, trailer, fmt.Errorf("open database file: %w", err)
	}
	defer func() { _ = dbFile.Close() }()

	// Open WAL file if we have overriding WAL frames.
	var walFile *os.File
	if len(walFrameOffsets) > 0 {
		if walFile, err = os.Open(db.WALPath()); err != nil {
			return header, trailer, fmt.Errorf("open wal file: %w", err)
		}
		defer func() { _ = walFile.Close() }()
	}

	// Write current database state to an LTX writer.
	enc := ltx.NewEncoder(dst)
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		PageSize:  pageSize,
		Commit:    pageN,
		MinTXID:   1,
		MaxTXID:   pos.TXID,
		Timestamp: db.Now().UnixMilli(),
	}); err != nil {
		return header, trailer, fmt.Errorf("encode ltx header: %w", err)
	}

	// Write page frames.
	pageData := make([]byte, pageSize)
	var chksum uint64
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		// Read from WAL if page exists in offset map. Otherwise read from DB.
		if walFrameOffset, ok := walFrameOffsets[pgno]; ok {
			if _, err := walFile.Seek(walFrameOffset+WALFrameHeaderSize, io.SeekStart); err != nil {
				return header, trailer, fmt.Errorf("seek wal page: %w", err)
			} else if _, err := io.ReadFull(walFile, pageData); err != nil {
				return header, trailer, fmt.Errorf("read wal page: %w", err)
			}
		} else {
			if _, err := dbFile.Seek(int64(pgno-1)*int64(pageSize), io.SeekStart); err != nil {
				return header, trailer, fmt.Errorf("seek database page: %w", err)
			} else if _, err := io.ReadFull(dbFile, pageData); err != nil {
				return header, trailer, fmt.Errorf("read database page: %w", err)
			}
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageData); err != nil {
			return header, trailer, fmt.Errorf("encode page frame: %w", err)
		}

		chksum ^= ltx.ChecksumPage(pgno, pageData)
	}

	// Set the database checksum before we write the trailer.
	postApplyChecksum := ltx.ChecksumFlag | chksum
	if postApplyChecksum != pos.PostApplyChecksum {
		return header, trailer, fmt.Errorf("snapshot checksum mismatch at tx %s: %x <> %x", ltx.FormatTXID(pos.TXID), postApplyChecksum, pos.PostApplyChecksum)
	}
	enc.SetPostApplyChecksum(postApplyChecksum)

	if err := enc.Close(); err != nil {
		return header, trailer, fmt.Errorf("close ltx encoder: %w", err)
	}

	return enc.Header(), enc.Trailer(), nil
}

// EnforceRetention removes all LTX files created before minTime.
func (db *DB) EnforceRetention(ctx context.Context, minTime time.Time) error {
	// Collect all LTX files.
	ents, err := db.ReadLTXDir()
	if err != nil {
		return fmt.Errorf("read ltx dir: %w", err)
	} else if len(ents) == 0 {
		return nil // no LTX files, exit
	}

	// Ensure the latest LTX file is not removed.
	ents = ents[:len(ents)-1]

	// Delete all files that are before the minimum time.
	var totalN int
	var totalSize int64
	for _, ent := range ents {
		// Check if file qualifies for deletion.
		fi, err := ent.Info()
		if err != nil {
			return fmt.Errorf("info: %w", err)
		} else if fi.ModTime().After(minTime) {
			totalN++
			totalSize += fi.Size()
			continue // after minimum time, skip
		}

		// Remove file if it passes all the checks.
		filename := filepath.Join(db.LTXDir(), ent.Name())
		if err := os.Remove(filename); err != nil {
			return err
		}

		// Update metrics.
		dbLTXReapCountMetricVec.WithLabelValues(db.name).Inc()
	}

	// Reset metrics for LTX disk usage.
	dbLTXCountMetricVec.WithLabelValues(db.name).Set(float64(totalN))
	dbLTXBytesMetricVec.WithLabelValues(db.name).Set(float64(totalSize))

	return nil
}

type dbVarJSON struct {
	Name     string `json:"name"`
	TXID     string `json:"txid"`
	Checksum string `json:"checksum"`

	Locks struct {
		Pending  string `json:"pending"`
		Shared   string `json:"shared"`
		Reserved string `json:"reserved"`

		Write   string `json:"write"`
		Ckpt    string `json:"ckpt"`
		Recover string `json:"recover"`
		Read0   string `json:"read0"`
		Read1   string `json:"read1"`
		Read2   string `json:"read2"`
		Read3   string `json:"read3"`
		Read4   string `json:"read4"`
		DMS     string `json:"dms"`
	} `json:"locks"`
}

func buildJournalPageMap(f *os.File, pageSize uint32) (map[uint32]uint64, error) {
	r := NewJournalReader(f, pageSize)

	// Generate a map of pages and their new checksums.
	m := make(map[uint32]uint64)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	for i := 0; ; i++ {
		if err := r.Next(); err == io.EOF {
			return m, nil
		} else if err != nil {
			return m, fmt.Errorf("journal segment(%d): %w", i, err)
		}

		for j := 0; ; j++ {
			pgno, data, err := r.ReadFrame()
			if err == io.EOF {
				break
			} else if err != nil {
				return m, fmt.Errorf("journal segment(%d) frame(%d): %w", i, j, err)
			}

			// Calculate LTX page checksum and add it to the map.
			chksum := ltx.ChecksumPage(pgno, data)
			m[pgno] = chksum
		}
	}
}

// JouralReader represents a reader of the SQLite journal file format.
type JournalReader struct {
	f      *os.File
	fi     os.FileInfo // cached file info
	offset int64       // read offset
	frame  []byte      // frame buffer

	frameN     int32  // Number of pages in the segment
	nonce      uint32 // A random nonce for the checksum
	commit     uint32 // Initial size of the database in pages
	sectorSize uint32 // Size of the disk sectors
	pageSize   uint32 // Size of each page, in bytes
}

// JournalReader returns a new instance of JournalReader.
func NewJournalReader(f *os.File, pageSize uint32) *JournalReader {
	return &JournalReader{
		f:        f,
		pageSize: pageSize,
	}
}

// DatabaseSize returns the size of the database before the journal transaction, in bytes.
func (r *JournalReader) DatabaseSize() int64 {
	return int64(r.commit) * int64(r.pageSize)
}

// Next reads the next segment of the journal. Returns io.EOF if no more segments exist.
func (r *JournalReader) Next() (err error) {
	// Determine journal size on initial call.
	if r.fi == nil {
		if r.fi, err = r.f.Stat(); err != nil {
			return err
		}
	}

	// Ensure offset is sector-aligned.
	r.offset = journalHeaderOffset(r.offset, int64(r.sectorSize))

	// Read full header.
	hdr := make([]byte, SQLITE_JOURNAL_HEADER_SIZE)
	if _, err := internal.ReadFullAt(r.f, hdr, r.offset); err == io.EOF || err == io.ErrUnexpectedEOF {
		return io.EOF // no header or partial header
	} else if err != nil {
		return err
	}

	// Exit if PERSIST journal mode has cleared the header for finalization.
	if isByteSliceZero(hdr) {
		return io.EOF
	}

	// After the first segment, we require the magic bytes.
	if r.offset > 0 && !bytes.Equal(hdr[:8], []byte(SQLITE_JOURNAL_HEADER_STRING)) {
		return io.EOF
	}

	// Read number of frames in journal segment. Set to -1 if no-sync was set
	// and set to 0 if the journal was not sync'd. In these two cases we will
	// calculate the frame count based on the journal size.
	r.frameN = int32(binary.BigEndian.Uint32(hdr[8:]))
	if r.frameN == -1 {
		r.frameN = int32((r.fi.Size() - int64(r.sectorSize)) / int64(r.pageSize))
	} else if r.frameN == 0 {
		r.frameN = int32((r.fi.Size() - r.offset) / int64(r.pageSize))
	}

	// Read remaining fields from header.
	r.nonce = binary.BigEndian.Uint32(hdr[12:])  // cksumInit
	r.commit = binary.BigEndian.Uint32(hdr[16:]) // dbSize

	// Only read sector and page size from first journal header.
	if r.offset == 0 {
		r.sectorSize = binary.BigEndian.Uint32(hdr[20:])

		// Use page size from journal reader, if set to 0.
		pageSize := binary.BigEndian.Uint32(hdr[24:])
		if pageSize == 0 {
			pageSize = r.pageSize
		}
		if pageSize != r.pageSize {
			return fmt.Errorf("journal header page size (%d) does not match database (%d)", pageSize, r.pageSize)
		}
	}

	// Exit if file doesn't have more than the initial sector.
	if r.offset+int64(r.sectorSize) > r.fi.Size() {
		return io.EOF
	}

	// Move journal offset to first sector.
	r.offset += int64(r.sectorSize)

	// Create a buffer to read the segment frames.
	r.frame = make([]byte, r.pageSize+4+4)

	return nil
}

// ReadFrame returns the page number and page data for the next frame.
// Returns io.EOF after the last frame. Page data should not be retained.
func (r *JournalReader) ReadFrame() (pgno uint32, data []byte, err error) {
	// No more frames, exit.
	if r.frameN == 0 {
		return 0, nil, io.EOF
	}

	// Read the next frame from the journal.
	n, err := internal.ReadFullAt(r.f, r.frame, r.offset)
	if err == io.ErrUnexpectedEOF {
		return 0, nil, io.EOF
	} else if err != nil {
		return 0, nil, err
	}

	pgno = binary.BigEndian.Uint32(r.frame[0:])
	data = r.frame[4 : len(r.frame)-4]
	chksum := binary.BigEndian.Uint32(r.frame[len(r.frame)-4:])

	if chksum != JournalChecksum(data, r.nonce) {
		return 0, nil, io.EOF
	}

	r.frameN--
	r.offset += int64(n)

	return pgno, data, nil
}

// journalHeaderOffset returns a sector-aligned offset.
func journalHeaderOffset(offset, sectorSize int64) int64 {
	if offset == 0 {
		return 0
	}
	return ((offset-1)/sectorSize + 1) * sectorSize
}

// readAndVerifyLTXFile reads an LTX file and verifies its integrity.
// Returns the header & the trailer from the file.
func readAndVerifyLTXFile(filename string) (ltx.Header, ltx.Trailer, error) {
	f, err := os.Open(filename)
	if err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}
	defer func() { _ = f.Close() }()

	r := ltx.NewReader(f)
	if _, err := io.Copy(io.Discard, r); err != nil {
		return ltx.Header{}, ltx.Trailer{}, err
	}
	return r.Header(), r.Trailer(), nil
}

// TrimName removes "-journal", "-shm" or "-wal" from the given name.
func TrimName(name string) string {
	if suffix := "-journal"; strings.HasSuffix(name, suffix) {
		name = strings.TrimSuffix(name, suffix)
	}
	if suffix := "-wal"; strings.HasSuffix(name, suffix) {
		name = strings.TrimSuffix(name, suffix)
	}
	if suffix := "-shm"; strings.HasSuffix(name, suffix) {
		name = strings.TrimSuffix(name, suffix)
	}
	return name
}

// errorKeyValue returns a key/value pair of the error. Returns a blank string if err is empty.
func errorKeyValue(err error) string {
	if err == nil {
		return ""
	}
	return "err=" + err.Error()
}

// Database metrics.
var (
	dbTXIDMetricVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litefs_db_txid",
		Help: "Current transaction ID.",
	}, []string{"db"})

	dbDatabaseWriteCountMetricVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litefs_db_database_write_count",
		Help: "Number of writes to the database file.",
	}, []string{"db"})

	dbJournalWriteCountMetricVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litefs_db_journal_write_count",
		Help: "Number of writes to the journal file.",
	}, []string{"db"})

	dbWALWriteCountMetricVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litefs_db_wal_write_count",
		Help: "Number of writes to the WAL file.",
	}, []string{"db"})

	dbSHMWriteCountMetricVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litefs_db_shm_write_count",
		Help: "Number of writes to the shared memory file.",
	}, []string{"db"})

	dbCommitCountMetricVec = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "litefs_db_commit_count",
		Help: "Number of database commits.",
	}, []string{"db"})

	dbLTXCountMetricVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litefs_db_ltx_count",
		Help: "Number of LTX files on disk.",
	}, []string{"db"})

	dbLTXBytesMetricVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litefs_db_ltx_bytes",
		Help: "Number of bytes used by LTX files on disk.",
	}, []string{"db"})

	dbLTXReapCountMetricVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litefs_db_ltx_reap_count",
		Help: "Number of LTX files removed by retention.",
	}, []string{"db"})

	dbLatencySecondsMetricVec = promauto.NewGaugeVec(prometheus.GaugeOpts{
		Name: "litefs_db_latency_seconds",
		Help: "Latency between generating an LTX file and consuming it.",
	}, []string{"db"})
)
