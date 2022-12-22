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
	"unsafe"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/ltx"
)

// DB represents a SQLite database.
type DB struct {
	store    *Store            // parent store
	name     string            // name of database
	path     string            // full on-disk path
	pageSize uint32            // database page size, if known
	pageN    uint32            // database size, in pages
	pos      atomic.Value      // current tx position (Pos)
	mode     DBMode            // database journaling mode (rollback, wal)
	chksums  map[uint32]uint64 // database page checksums

	dirtyPageSet map[uint32]struct{}

	wal struct {
		offset           int64               // offset of the start of the transaction
		byteOrder        binary.ByteOrder    // determine by WAL header magic
		salt1, salt2     uint32              // current WAL header salt values
		chksum1, chksum2 uint32              // WAL checksum values at wal.offset
		frameOffsets     map[uint32]int64    // WAL frame offset of the last version of a given pgno before current tx
		chksums          map[uint32][]uint64 // wal page checksums
	}
	shmMu sync.Mutex // shm invalidation can trigger mmap write that we need to avoid

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
		store:   store,
		name:    name,
		path:    path,
		chksums: make(map[uint32]uint64),

		dirtyPageSet: make(map[uint32]struct{}),

		Now: time.Now,
	}
	db.pos.Store(Pos{})
	db.wal.frameOffsets = make(map[uint32]int64)
	db.wal.chksums = make(map[uint32][]uint64)
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

	// Determine the last LTX file to replay from, if any.
	ltxFilename, err := db.maxLTXFile(context.Background())
	if err != nil {
		return fmt.Errorf("max ltx file: %w", err)
	}

	// Sync up WAL and last LTX file, if they both exist.
	if ltxFilename != "" {
		if err := db.syncWALToLTX(context.Background(), ltxFilename); err != nil {
			return fmt.Errorf("sync wal to ltx: %w", err)
		}
	}

	// Reset the rollback journal and/or WAL.
	if err := db.recover(context.Background()); err != nil {
		return fmt.Errorf("recover: %w", err)
	}

	// Verify database header & initialize checksums.
	if err := db.initDatabaseFile(); err != nil {
		return fmt.Errorf("init database file: %w", err)
	}

	// Apply the last LTX file so our checksums match if there was a failure in
	// between the LTX commit and journal/WAL commit.
	if ltxFilename != "" {
		if err := db.ApplyLTX(context.Background(), ltxFilename); err != nil {
			return fmt.Errorf("recover ltx: %w", err)
		}
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

// Recover forces a rollback (journal) or checkpoint (wal).
func (db *DB) Recover(ctx context.Context) error {
	guard, err := db.AcquireWriteLock(ctx)
	if err != nil {
		return err
	}
	defer guard.Unlock()
	return db.recover(ctx)
}

func (db *DB) recover(ctx context.Context) (err error) {
	defer func() {
		TraceLog.Printf("[Recover(%s)]: %s", db.name, errorKeyValue(err))
	}()

	// If a journal file exists, rollback the last transaction so that we
	// are in a consistent state before applying our last LTX file. Otherwise
	// we could have a partial transaction in our database, apply our LTX, and
	// then the SQLite client will recover from the journal and corrupt everything.
	//
	// See: https://github.com/superfly/litefs/issues/134
	if err := db.rollbackJournal(ctx); err != nil {
		return fmt.Errorf("rollback journal: %w", err)
	}

	// Copy the WAL file back to the main database. This ensures that we can
	// compute the checksum only using the database file instead of having to
	// first compute the latest page set from the WAL to overlay.
	if err := db.checkpoint(ctx); err != nil {
		return fmt.Errorf("checkpoint: %w", err)
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

	// Resize database to size before journal transaction, if a valid header exists.
	if r.IsValid() {
		if err := db.truncateDatabase(dbFile, r.commit); err != nil {
			return err
		}
	}

	if err := dbFile.Sync(); err != nil {
		return err
	} else if err := dbFile.Close(); err != nil {
		return err
	}

	if err := journalFile.Close(); err != nil {
		return err
	} else if err := os.Remove(db.JournalPath()); err != nil {
		return err
	}

	if invalidator := db.store.Invalidator; invalidator != nil {
		if err := invalidator.InvalidateEntry(db.name + "-journal"); err != nil {
			return fmt.Errorf("invalidate journal: %w", err)
		}
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
		if err := db.writeDatabasePage(dbFile, pgno, data); err != nil {
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

			if err := db.writeDatabasePage(dbFile, pgno, buf); err != nil {
				return fmt.Errorf("write db page %d: %w", pgno, err)
			}
		}

		if err := db.truncateDatabase(dbFile, commit); err != nil {
			return fmt.Errorf("truncate: %w", err)
		}

		// Save the size of the database, in pages, based on last commit.
		db.pageN = commit
	}

	// Remove WAL file.
	if err := db.TruncateWAL(ctx, 0); err != nil {
		return fmt.Errorf("truncate wal: %w", err)
	}

	// Clear per-page checksums within WAL.
	db.wal.chksums = make(map[uint32][]uint64)

	// Update the SHM file.
	if err := db.updateSHM(ctx); err != nil {
		return fmt.Errorf("update shm: %w", err)
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

// maxLTXFile returns the filename of the highest LTX file.
func (db *DB) maxLTXFile(ctx context.Context) (string, error) {
	ents, err := os.ReadDir(db.LTXDir())
	if err != nil {
		return "", err
	}

	var max uint64
	var filename string
	for _, ent := range ents {
		_, maxTXID, err := ltx.ParseFilename(ent.Name())
		if err != nil {
			continue
		} else if maxTXID <= max {
			continue
		}

		// Save filename with the highest TXID.
		max, filename = maxTXID, filepath.Join(db.LTXDir(), ent.Name())
	}
	return filename, nil
}

// syncWALToLTX truncates the WAL file to the last LTX file if the WAL info
// in the LTX header does not match. This protects against a hard shutdown
// where a WAL file was sync'd past the last LTX file.
func (db *DB) syncWALToLTX(ctx context.Context, ltxFilename string) error {
	// Open last LTX file.
	ltxFile, err := os.Open(ltxFilename)
	if err != nil {
		return err
	}
	defer func() { _ = ltxFile.Close() }()

	// Read header from LTX file to determine WAL fields.
	// This also validates the LTX file before it gets processed by ApplyLTX().
	r := ltx.NewReader(ltxFile)
	if _, err := io.Copy(io.Discard, r); err != nil {
		return fmt.Errorf("validate ltx: %w", err)
	}
	ltxWALSize := r.Header().WALOffset + r.Header().WALSize

	// Open WAL file, ignore if it doesn't exist.
	walFile, err := os.OpenFile(db.WALPath(), os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		log.Printf("wal-sync: no wal file exists on %q, skipping sync with ltx", db.name)
		return nil // no wal file, nothing to do
	} else if err != nil {
		return err
	}
	defer func() { _ = walFile.Close() }()

	// Determine WAL size.
	fi, err := walFile.Stat()
	if err != nil {
		return err
	}

	// Read WAL header.
	hdr := make([]byte, WALHeaderSize)
	if _, err := internal.ReadFullAt(walFile, hdr, 0); err == io.EOF || err == io.ErrUnexpectedEOF {
		log.Printf("wal-sync: short wal file exists on %q, skipping sync with ltx", db.name)
		return nil // short WAL header, skip
	} else if err != nil {
		return err
	}

	// If WAL salt doesn't match the LTX WAL salt then the WAL has been
	// restarted and we need to remove it. We are just renaming it for now so
	// we can debug in case this happens.
	salt1 := binary.BigEndian.Uint32(hdr[16:])
	salt2 := binary.BigEndian.Uint32(hdr[20:])
	if salt1 != r.Header().WALSalt1 || salt2 != r.Header().WALSalt2 {
		log.Printf("wal-sync: wal salt mismatch on %q, removing wal", db.name)
		if err := os.Rename(db.WALPath(), db.WALPath()+".removed"); err != nil {
			return fmt.Errorf("wal-sync: rename wal file with salt mismatch: %w", err)
		}
		return nil
	}

	// If the salt matches then we need to make sure we are at least up to the
	// start of the last LTX transaction.
	if fi.Size() < r.Header().WALOffset {
		return fmt.Errorf("wal-sync: short wal size (%d bytes) for %q, last ltx offset at %d bytes", fi.Size(), db.name, r.Header().WALOffset)
	}

	// Resize WAL back to size in the LTX file.
	if fi.Size() > ltxWALSize {
		log.Printf("wal-sync: truncating wal of %q from %d bytes to %d bytes to match ltx", db.name, fi.Size(), ltxWALSize)
		if err := walFile.Truncate(ltxWALSize); err != nil {
			return fmt.Errorf("truncate wal: %w", err)
		}
		return nil
	}

	log.Printf("wal-sync: database %q has wal size of %d bytes within range of ltx file (@%d, %d bytes)", db.name, fi.Size(), r.Header().WALOffset, r.Header().WALSize)
	return nil
}

// initDatabaseFile opens and validates the database file, if it exists.
// The journal & WAL should not exist at this point. The journal should be
// rolled back and the WAL should be checkpointed.
func (db *DB) initDatabaseFile() error {
	f, err := os.Open(db.DatabasePath())
	if os.IsNotExist(err) {
		log.Printf("database file does not exist on initialization: %s", db.DatabasePath())
		return nil // no database file yet
	} else if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	hdr, _, err := readSQLiteDatabaseHeader(f)
	if err == io.EOF {
		log.Printf("database file is zero length on initialization: %s", db.DatabasePath())
		return nil // no contents yet
	} else if err != nil {
		return fmt.Errorf("cannot read database header: %w", err)
	}
	db.pageSize = hdr.PageSize
	db.pageN = hdr.PageN

	assert(db.pageSize > 0, "page size must be greater than zero")

	// Build per-page checksum map for existing pages. The database could be
	// short compared to the page count in the header so just checksum what we
	// can. The database may recover in applyLTX() so we'll do validation then.
	buf := make([]byte, db.pageSize)
	db.chksums = make(map[uint32]uint64)
	for pgno := uint32(1); pgno <= db.pageN; pgno++ {
		offset := int64(pgno-1) * int64(db.pageSize)
		if _, err := internal.ReadFullAt(f, buf, offset); err == io.EOF || err == io.ErrUnexpectedEOF {
			log.Printf("database checksum ending early at page %d of %d ", pgno-1, db.pageN)
			break
		} else if err != nil {
			return fmt.Errorf("read database page %d: %w", pgno, err)
		}

		db.chksums[pgno] = ltx.ChecksumPage(pgno, buf)
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

	// Process the actual file system truncation.
	if f, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666); err != nil {
		return err
	} else if err := db.truncateDatabase(f, pageN); err != nil {
		_ = f.Close()
		return err
	} else if err := f.Close(); err != nil {
		return err
	}

	return nil
}

// truncateDatabase truncates the database to a given page count.
func (db *DB) truncateDatabase(f *os.File, pageN uint32) (err error) {
	prevPageN := db.pageN

	defer func() {
		TraceLog.Printf("[TruncateDatabase(%s)]: pageN=%d prevPageN=%d pageSize=%d %s", db.name, pageN, prevPageN, db.pageSize, errorKeyValue(err))
	}()

	if err := f.Truncate(int64(pageN) * int64(db.pageSize)); err != nil {
		return err
	} else if err := f.Sync(); err != nil {
		return err
	}

	// Remove checksums after end of database on success.
	for pgno := pageN + 1; pgno <= prevPageN; pgno++ {
		pageChksum := db.chksums[pgno]
		TraceLog.Printf("[TruncatePage(%s)]: pgno=%d chksum=%016x", db.name, pgno, pageChksum)
		delete(db.chksums, pgno)
	}

	// Update page count.
	db.pageN = pageN

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

	// Perform write on handle.
	if err := db.writeDatabasePage(f, pgno, data); err != nil {
		return err
	}

	return nil
}

// writeDatabasePage writes a page to the database file.
func (db *DB) writeDatabasePage(f *os.File, pgno uint32, data []byte) (err error) {
	var prevChksum, newChksum uint64
	defer func() {
		TraceLog.Printf("[WriteDatabasePage(%s)]: pgno=%d chksum=%016x prev=%016x %s", db.name, pgno, newChksum, prevChksum, errorKeyValue(err))
	}()

	assert(db.pageSize != 0, "page size required")
	if len(data) != int(db.pageSize) {
		return fmt.Errorf("database write (%d bytes) must be a single page (%d bytes)", len(data), db.pageSize)
	}

	// Obtain checksums before write so trace log can show it, even on error.
	prevChksum = db.chksums[pgno]
	newChksum = ltx.ChecksumPage(pgno, data)

	// Issue write to database.
	offset := (int64(pgno) - 1) * int64(db.pageSize)
	if _, err := f.WriteAt(data, offset); err != nil {
		return err
	}
	dbDatabaseWriteCountMetricVec.WithLabelValues(db.name).Inc()

	// Update in-memory checksum.
	db.chksums[pgno] = newChksum

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
	err := db.CommitJournal(ctx, JournalModeTruncate)
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
	err := db.CommitJournal(ctx, JournalModeDelete)
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
func (db *DB) WriteJournalAt(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (err error) {
	defer func() {
		var buf []byte
		if len(data) == 4 { // pgno or chksum
			buf = data
		} else if offset == 0 { // show initial header
			buf = data
			if len(data) > SQLITE_JOURNAL_HEADER_SIZE {
				buf = data[:SQLITE_JOURNAL_HEADER_SIZE]
			}
		}

		TraceLog.Printf("[WriteJournalAt(%s)]: offset=%d size=%d data=%x owner=%d %s", db.name, offset, len(data), buf, owner, errorKeyValue(err))
	}()

	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	}

	// Set the page size on initial journal header write.
	if offset == 0 && len(data) >= SQLITE_JOURNAL_HEADER_SIZE && db.pageSize == 0 {
		db.pageSize = binary.BigEndian.Uint32(data[24:])
	}

	dbJournalWriteCountMetricVec.WithLabelValues(db.name).Inc()

	// Assume this is a PERSIST commit if the initial header bytes are cleared.
	if offset == 0 && len(data) == SQLITE_JOURNAL_HEADER_SIZE && isByteSliceZero(data) {
		if err := db.CommitJournal(ctx, JournalModePersist); err != nil {
			return fmt.Errorf("commit journal (PERSIST): %w", err)
		}
	}

	// Passthrough write
	_, err = f.WriteAt(data, offset)
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
func (db *DB) TruncateWAL(ctx context.Context, size int64) (err error) {
	defer func() { TraceLog.Printf("[TruncateWAL(%s)]: size=%d %s", db.name, size, errorKeyValue(err)) }()

	if size != 0 {
		return fmt.Errorf("wal can only be truncated to zero")
	}
	if err := os.Truncate(db.WALPath(), size); err != nil {
		return err
	}

	// Clear all per-page checksums for the WAL.
	db.wal.frameOffsets = make(map[uint32]int64)
	db.wal.chksums = make(map[uint32][]uint64)

	return nil
}

// RemoveWAL deletes the WAL file from disk.
func (db *DB) RemoveWAL(ctx context.Context) (err error) {
	defer func() { TraceLog.Printf("[RemoveWAL(%s)]: %s", db.name, errorKeyValue(err)) }()

	if err := os.Remove(db.WALPath()); err != nil {
		return err
	}

	// Clear all per-page checksums for the WAL.
	db.wal.frameOffsets = make(map[uint32]int64)
	db.wal.chksums = make(map[uint32][]uint64)

	return nil
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

	// WAL header writes always start at a zero offset and are 32 bytes in size.
	frameSize := WALFrameHeaderSize + int64(db.pageSize)
	if offset == 0 {
		if err := db.writeWALHeader(ctx, f, data, offset, owner); err != nil {
			return fmt.Errorf("wal header: %w", err)
		}
		return nil
	}

	// WAL frame headers can be full 24-byte headers or partial ones.
	if frameOffset := (offset - WALHeaderSize) % frameSize; frameOffset < WALFrameHeaderSize {
		if err := db.writeWALFrameHeader(ctx, f, data, offset, frameOffset, owner); err != nil {
			return fmt.Errorf("wal frame header: %w", err)
		}
		return nil
	}

	// Otherwise this is a write to the page data portion of the WAL frame.
	if err := db.writeWALFrameData(ctx, f, data, offset, owner); err != nil {
		return fmt.Errorf("wal frame data: %w", err)
	}
	return nil
}

func (db *DB) writeWALHeader(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (err error) {
	defer func() {
		TraceLog.Printf("[WriteWALHeader(%s)]: offset=%d size=%d salt1=%08x salt2=%08x chksum1=%08x chksum2=%08x owner=%d %s",
			db.name, offset, len(data), db.wal.salt1, db.wal.salt2, db.wal.chksum1, db.wal.chksum2, owner, errorKeyValue(err))
	}()

	if len(data) != WALHeaderSize {
		return fmt.Errorf("WAL header write must be 32 bytes in size, received %d", len(data))
	}

	// Determine byte order of checksums.
	switch magic := binary.BigEndian.Uint32(data[0:]); magic {
	case 0x377f0682:
		db.wal.byteOrder = binary.LittleEndian
	case 0x377f0683:
		db.wal.byteOrder = binary.BigEndian
	default:
		return fmt.Errorf("invalid magic: %x", magic)
	}

	// Set remaining WAL fields.
	db.wal.offset = WALHeaderSize
	db.wal.salt1 = binary.BigEndian.Uint32(data[16:])
	db.wal.salt2 = binary.BigEndian.Uint32(data[20:])
	db.wal.chksum1 = binary.BigEndian.Uint32(data[24:])
	db.wal.chksum2 = binary.BigEndian.Uint32(data[28:])
	db.wal.frameOffsets = make(map[uint32]int64)
	db.wal.chksums = make(map[uint32][]uint64)

	// Passthrough write to underlying WAL file.
	_, err = f.WriteAt(data, offset)
	return err
}

func (db *DB) writeWALFrameHeader(ctx context.Context, f *os.File, data []byte, offset, frameOffset int64, owner uint64) (err error) {
	var pgno, commit, salt1, salt2, chksum1, chksum2 uint32
	var hexdata string
	if frameOffset == 0 && len(data) == WALFrameHeaderSize {
		pgno = binary.BigEndian.Uint32(data[0:])
		commit = binary.BigEndian.Uint32(data[4:])
		salt1 = binary.BigEndian.Uint32(data[8:])
		salt2 = binary.BigEndian.Uint32(data[12:])
		chksum1 = binary.BigEndian.Uint32(data[16:])
		chksum2 = binary.BigEndian.Uint32(data[20:])
	} else {
		hexdata = fmt.Sprintf("%x", data)
	}

	defer func() {
		if hexdata != "" {
			TraceLog.Printf("[WriteWALFrameHeader(%s)]: offset=%d size=%d data=%s owner=%d %s",
				db.name, offset, len(data), hexdata, owner, errorKeyValue(err))
		} else {
			TraceLog.Printf("[WriteWALFrameHeader(%s)]: offset=%d size=%d pgno=%d commit=%d salt1=%08x salt2=%08x chksum1=%08x chksum2=%08x owner=%d %s",
				db.name, offset, len(data), pgno, commit, salt1, salt2, chksum1, chksum2, owner, errorKeyValue(err))
		}
	}()

	// Prevent SQLite from writing before the current WAL position.
	if offset < db.wal.offset {
		return fmt.Errorf("cannot write wal frame header @%d before current WAL position @%d", offset, db.wal.offset)
	}

	// Passthrough write to underlying WAL file.
	_, err = f.WriteAt(data, offset)
	return err
}

func (db *DB) writeWALFrameData(ctx context.Context, f *os.File, data []byte, offset int64, owner uint64) (err error) {
	defer func() {
		TraceLog.Printf("[WriteWALFrameData(%s)]: offset=%d size=%d owner=%d %s", db.name, offset, len(data), owner, errorKeyValue(err))
	}()

	// Prevent SQLite from writing before the current WAL position.
	if offset < db.wal.offset {
		return fmt.Errorf("cannot write wal frame data @%d before current WAL position @%d", offset, db.wal.offset)
	}

	// Passthrough write to underlying WAL file.
	_, err = f.WriteAt(data, offset)
	return err
}

func (db *DB) buildTxFrameOffsets(walFile *os.File) (_ map[uint32]int64, commit, chksum1, chksum2 uint32, endOffset int64, err error) {
	m := make(map[uint32]int64)

	offset := db.wal.offset
	chksum1, chksum2 = db.wal.chksum1, db.wal.chksum2
	frame := make([]byte, WALFrameHeaderSize+int64(db.pageSize))
	for i := 0; ; i++ {
		// Read frame data & exit if we hit the end of file.
		if _, err := internal.ReadFullAt(walFile, frame, offset); err == io.EOF || err == io.ErrUnexpectedEOF {
			TraceLog.Printf("[buildTxFrames(%s)]: msg=read-error offset=%d size=%d err=%q",
				db.name, offset, len(frame), err)
			return nil, 0, 0, 0, 0, errNoTransaction
		} else if err != nil {
			return nil, 0, 0, 0, 0, fmt.Errorf("read wal frame: %w", err)
		}

		// If salt doesn't match then we've run into a previous WAL's data.
		salt1 := binary.BigEndian.Uint32(frame[8:])
		salt2 := binary.BigEndian.Uint32(frame[12:])
		if db.wal.salt1 != salt1 || db.wal.salt2 != salt2 {
			TraceLog.Printf("[buildTxFrames(%s)]: msg=salt-mismatch offset=%d hdr-salt1=%08x hdr-salt2=%08x frame-salt1=%08x frame-salt2=%08x",
				db.name, offset, db.wal.salt1, db.wal.salt2, salt1, salt2)
			return nil, 0, 0, 0, 0, errNoTransaction
		}

		// Verify checksum
		fchksum1 := binary.BigEndian.Uint32(frame[16:])
		fchksum2 := binary.BigEndian.Uint32(frame[20:])
		chksum1, chksum2 = WALChecksum(db.wal.byteOrder, chksum1, chksum2, frame[:8])  // frame header
		chksum1, chksum2 = WALChecksum(db.wal.byteOrder, chksum1, chksum2, frame[24:]) // frame data
		if chksum1 != fchksum1 || chksum2 != fchksum2 {
			TraceLog.Printf("[buildTxFrames(%s)]: msg=chksum-mismatch offset=%d chksum1=%08x chksum2=%08x frame-chksum1=%08x frame-chksum2=%08x",
				db.name, offset, chksum1, chksum2, fchksum1, fchksum2)
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
func (db *DB) CommitWAL(ctx context.Context) (err error) {
	var msg string
	var commit uint32
	var txPageCount int
	var pos Pos
	prevPos := db.Pos()
	prevPageN := db.pageN
	defer func() {
		TraceLog.Printf("[CommitWAL(%s)]: pos=%s prevPos=%s pages=%d commit=%d prevPageN=%d pageSize=%d msg=%q %s\n\n",
			db.name, pos, prevPos, txPageCount, commit, prevPageN, db.pageSize, msg, errorKeyValue(err))
	}()
	walFrameSize := int64(WALFrameHeaderSize + db.pageSize)

	TraceLog.Printf("[CommitWALBegin(%s)]: prev=%s offset=%d salt1=%08x salt2=%08x chksum1=%08x chksum2=%08x",
		db.name, prevPos, db.wal.offset, db.wal.salt1, db.wal.salt2, db.wal.chksum1, db.wal.chksum2)

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
		msg = "no transaction"
		return nil
	} else if err != nil {
		return fmt.Errorf("build tx frame offsets: %w", err)
	}
	txPageCount = len(txFrameOffsets)

	dbFile, err := os.Open(db.DatabasePath())
	if err != nil {
		return fmt.Errorf("cannot open database file: %w", err)
	}
	defer func() { _ = dbFile.Close() }()

	// Determine transaction ID of the in-process transaction.
	txID := prevPos.TXID + 1

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
		PreApplyChecksum: prevPos.PostApplyChecksum,
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
	newWALChksums := make(map[uint32]uint64)
	lockPgno := ltx.LockPgno(db.pageSize)
	for _, pgno := range pgnos {
		if pgno == lockPgno {
			TraceLog.Printf("[CommitWALPage(%s)]: pgno=%d SKIP(LOCK_PAGE)\n", db.name, pgno)
			continue
		}

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

		// Update per-page checksum.
		prevPageChksum, _ := db.pageChecksum(pgno, db.pageN, nil)
		pageChksum := ltx.ChecksumPage(pgno, frame[WALFrameHeaderSize:])
		newWALChksums[pgno] = pageChksum

		TraceLog.Printf("[CommitWALPage(%s)]: pgno=%d chksum=%016x prev=%016x\n", db.name, pgno, pageChksum, prevPageChksum)
	}

	// Remove checksum of truncated pages.
	page := make([]byte, db.pageSize)
	for pgno := commit + 1; pgno <= prevPageN; pgno++ {
		if pgno == lockPgno {
			TraceLog.Printf("[CommitWALRemovePage(%s)]: pgno=%d SKIP(LOCK_PAGE)\n", db.name, pgno)
			continue
		}

		if err := db.readPage(dbFile, walFile, pgno, page); err != nil {
			return fmt.Errorf("read truncated page: pgno=%d err=%w", pgno, err)
		}

		// Clear per-page checksum.
		prevPageChksum, _ := db.pageChecksum(pgno, db.pageN, nil)
		pageChksum := ltx.ChecksumPage(pgno, page)
		if pageChksum != prevPageChksum {
			return fmt.Errorf("truncated page %d checksum mismatch: %016x <> %016x", pgno, pageChksum, prevPageChksum)
		}
		newWALChksums[pgno] = 0

		TraceLog.Printf("[CommitWALRemovePage(%s)]: pgno=%d prev=%016x\n", db.name, pgno, prevPageChksum)
	}

	// Calculate checksum after commit.
	postApplyChecksum, err := db.checksum(commit, newWALChksums)
	if err != nil {
		return fmt.Errorf("compute checksum: %w", err)
	}
	enc.SetPostApplyChecksum(postApplyChecksum)

	// Finish page block to compute checksum and then finish header block.
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

	// Append new checksums onto WAL set.
	for pgno, chksum := range newWALChksums {
		db.wal.chksums[pgno] = append(db.wal.chksums[pgno], chksum)
	}

	// Move the WAL position forward and reset the segment size.
	db.pageN = commit
	db.wal.offset = endOffset
	db.wal.chksum1 = chksum1
	db.wal.chksum2 = chksum2

	// Update transaction for database.
	pos = Pos{
		TXID:              enc.Header().MaxTXID,
		PostApplyChecksum: enc.Trailer().PostApplyChecksum,
	}
	if err := db.setPos(pos); err != nil {
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
		if chksum, err := db.onDiskChecksum(dbFile, walFile); err != nil {
			return fmt.Errorf("checksum (wal): %w", err)
		} else if chksum != postApplyChecksum {
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

	// Process WAL if we have an exclusive lock on WAL_WRITE_LOCK.
	if guardSet.Write().State() == RWMutexStateExclusive {
		if err := db.CommitWAL(ctx); err != nil {
			log.Printf("commit wal error: %s", err)
		}
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
	// Ignore writes that occur while the SHM is updating. This is a side effect
	// of SQLite using mmap() which can cause re-access to update it.
	if !db.shmMu.TryLock() {
		return len(data), nil
	}
	defer db.shmMu.Unlock()

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
func (db *DB) CommitJournal(ctx context.Context, mode JournalMode) (err error) {
	var pos Pos
	prevPos := db.Pos()
	prevPageN := db.pageN
	defer func() {
		TraceLog.Printf("[CommitJournal(%s)]: pos=%s prevPos=%s pageN=%d prevPageN=%d mode=%s %s\n\n",
			db.name, pos, prevPos, db.pageN, prevPageN, mode, errorKeyValue(err))
	}()

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
	txID := prevPos.TXID + 1

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
		PreApplyChecksum: prevPos.PostApplyChecksum,
	}); err != nil {
		return fmt.Errorf("cannot encode ltx header: %s", err)
	}

	// Remove WAL checksums. These shouldn't exist but remove them just in case.
	db.wal.chksums = make(map[uint32][]uint64)

	// Copy transactions from main database to the LTX file in sorted order.
	buf := make([]byte, db.pageSize)
	dbMode := DBModeRollback
	lockPgno := ltx.LockPgno(db.pageSize)
	for _, pgno := range pgnos {
		if pgno == lockPgno {
			TraceLog.Printf("[CommitJournalPage(%s)]: pgno=%d SKIP(LOCK_PAGE)\n", db.name, pgno)
			continue
		}

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
		pageChksum, ok := db.pageChecksum(pgno, commit, nil)
		if !ok {
			return fmt.Errorf("updated page checksum not found: pgno=%d", pgno)
		}
		bufChksum := ltx.ChecksumPage(pgno, buf)
		if bufChksum != pageChksum {
			return fmt.Errorf("updated page (%d) does not match in-memory checksum: %016x <> %016x (⊕%016x)", pgno, bufChksum, pageChksum, bufChksum^pageChksum)
		}

		TraceLog.Printf("[CommitJournalPage(%s)]: pgno=%d chksum=%016x %s", db.name, pgno, pageChksum, errorKeyValue(err))
	}

	// Remove all checksums after last page.
	for pgno := range db.chksums {
		if pgno <= commit {
			continue
		}

		if pgno == lockPgno {
			TraceLog.Printf("[CommitJournalRemovePage(%s)]: pgno=%d SKIP(LOCK_PAGE)\n", db.name, pgno)
			continue
		}

		pageChksum, _ := db.pageChecksum(pgno, db.pageN, nil)
		delete(db.chksums, pgno)
		TraceLog.Printf("[CommitJournalRemovePage(%s)]: pgno=%d chksum=%016x %s", db.name, pgno, pageChksum, errorKeyValue(err))
	}

	// Compute new database checksum.
	postApplyChecksum, err := db.checksum(commit, nil)
	if err != nil {
		return fmt.Errorf("compute checksum: %w", err)
	}
	enc.SetPostApplyChecksum(postApplyChecksum)

	// Finish page block to compute checksum and then finish header block.
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
	pos = Pos{
		TXID:              enc.Header().MaxTXID,
		PostApplyChecksum: enc.Trailer().PostApplyChecksum,
	}
	if err := db.setPos(pos); err != nil {
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
		if chksum, err := db.onDiskChecksum(dbFile, nil); err != nil {
			return fmt.Errorf("checksum (journal): %w", err)
		} else if chksum != postApplyChecksum {
			return fmt.Errorf("verification failed (journal): %016x <> %016x", chksum, postApplyChecksum)
		}
	}

	return nil
}

// onDiskChecksum calculates the LTX checksum directly from the on-disk database & WAL.
func (db *DB) onDiskChecksum(dbFile, walFile *os.File) (chksum uint64, err error) {
	if db.pageSize == 0 {
		return 0, fmt.Errorf("page size required for checksum")
	} else if db.pageN == 0 {
		return 0, fmt.Errorf("page count required for checksum")
	}

	// Compute the lock page once and skip it during checksumming.
	lockPgno := ltx.LockPgno(db.pageSize)

	data := make([]byte, db.pageSize)
	for pgno := uint32(1); pgno <= db.pageN; pgno++ {
		if pgno == lockPgno {
			continue
		}

		// Read from either the database file or the WAL depending if the page exists in the WAL.
		if offset, ok := db.wal.frameOffsets[pgno]; !ok {
			if _, err := internal.ReadFullAt(dbFile, data, int64(pgno-1)*int64(db.pageSize)); err != nil {
				return 0, fmt.Errorf("db read (pgno=%d): %w", pgno, err)
			}
		} else {
			if _, err := internal.ReadFullAt(walFile, data, offset+WALFrameHeaderSize); err != nil {
				return 0, fmt.Errorf("wal read (pgno=%d): %w", pgno, err)
			}
		}

		// Add the page to the rolling checksum.
		chksum = ltx.ChecksumFlag | (chksum ^ ltx.ChecksumPage(pgno, data))
	}

	return chksum, nil
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

func (db *DB) applyLTX(ctx context.Context, path string) (err error) {
	var hdr ltx.Header
	var trailer ltx.Trailer
	prevDBMode := db.mode
	defer func() {
		TraceLog.Printf("[ApplyLTX(%s)]: txid=%s-%s chksum=%016x-%16x commit=%d pageSize=%d timestamp=%s mode=(%s→%s) path=%s",
			db.name, ltx.FormatTXID(hdr.MinTXID), ltx.FormatTXID(hdr.MaxTXID), hdr.PreApplyChecksum, trailer.PostApplyChecksum, hdr.Commit, db.pageSize,
			time.UnixMilli(hdr.Timestamp).UTC().Format(time.RFC3339), prevDBMode, db.mode, filepath.Base(path))
	}()

	// Open database file for writing.
	dbFile, err := os.OpenFile(db.DatabasePath(), os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open database file: %w", err)
	}
	defer func() { _ = dbFile.Close() }()

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
	hdr = dec.Header()
	if db.pageSize == 0 {
		db.pageSize = dec.Header().PageSize
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
		if err := db.writeDatabasePage(dbFile, phdr.Pgno, pageBuf); err != nil {
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
	trailer = dec.Trailer()

	// Truncate database file to size after LTX file.
	if err := db.truncateDatabase(dbFile, dec.Header().Commit); err != nil {
		return fmt.Errorf("truncate database file: %w", err)
	}

	db.mode = dbMode

	// Ensure checksum matches the post-apply checksum.
	if chksum, err := db.checksum(dec.Header().Commit, nil); err != nil {
		return fmt.Errorf("compute checksum: %w", err)
	} else if chksum != dec.Trailer().PostApplyChecksum {
		return fmt.Errorf("database checksum %016x does not match LTX post-apply checksum %016x", chksum, dec.Trailer().PostApplyChecksum)
	}

	// Update transaction for database.
	if err := db.setPos(Pos{
		TXID:              dec.Header().MaxTXID,
		PostApplyChecksum: dec.Trailer().PostApplyChecksum,
	}); err != nil {
		return fmt.Errorf("set pos: %w", err)
	}

	// Rewrite SHM so that the transaction is visible.
	if err := db.updateSHM(ctx); err != nil {
		return fmt.Errorf("update shm: %w", err)
	}

	// Notify store of database change.
	db.store.MarkDirty(db.name)

	// Calculate latency since LTX file was written.
	latency := float64(time.Now().UnixMilli()-dec.Header().Timestamp) / 1000
	dbLatencySecondsMetricVec.WithLabelValues(db.name).Set(latency)

	return nil
}

// updateSHM recomputes the SHM header for a replica node (with no WAL frames).
func (db *DB) updateSHM(ctx context.Context) error {
	// This lock prevents an issue where triggering SHM invalidation in FUSE
	// causes a write to be issued through the mmap which overwrites our change.
	// This lock blocks that from occurring.
	db.shmMu.Lock()
	defer db.shmMu.Unlock()

	f, err := os.OpenFile(db.SHMPath(), os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return err
	}
	defer func() { _ = f.Close() }()

	// Read current header, if it exists.
	data := make([]byte, WALIndexBlockSize)
	if _, err := internal.ReadFullAt(f, data, 0); err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
		return fmt.Errorf("read shm header: %w", err)
	}

	// Read previous header. SHM uses native endianness so use unsafe to map to the struct.
	prevHdr := *(*walIndexHdr)(unsafe.Pointer(&data[0]))

	// Write header.
	hdr := walIndexHdr{
		version:     3007000,
		change:      prevHdr.change + 1,
		isInit:      1,
		bigEndCksum: prevHdr.bigEndCksum,
		pageSize:    encodePageSize(db.pageSize),
		pageN:       db.pageN,
		frameCksum:  [2]uint32{db.wal.chksum1, db.wal.chksum2},
		salt:        [2]uint32{db.wal.salt1, db.wal.salt2},
	}
	hdrBytes := (*[40]byte)(unsafe.Pointer(&hdr))[:]
	hdr.cksum[0], hdr.cksum[1] = WALChecksum(NativeEndian, 0, 0, hdrBytes)

	// Write two copies of header and then the checkpoint info.
	ckptInfo := walCkptInfo{
		readMark: [5]uint32{0x00000000, 0xffffffff, 0xffffffff, 0xffffffff, 0xffffffff},
	}
	copy(data[0:48], (*[48]byte)(unsafe.Pointer(&hdr))[:])
	copy(data[48:96], (*[48]byte)(unsafe.Pointer(&hdr))[:])
	copy(data[96:], (*[40]byte)(unsafe.Pointer(&ckptInfo))[:])

	// Overwrite the SHM index block.
	if _, err := f.WriteAt(data, 0); err != nil {
		return err
	} else if err := f.Truncate(int64(len(data))); err != nil {
		return err
	}

	// Invalidate page cache.
	if invalidator := db.store.Invalidator; invalidator != nil {
		if err := invalidator.InvalidateSHM(db); err != nil {
			return err
		}
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

	// Invalidate journal, if one exists.
	if err := db.invalidateJournal(JournalModePersist); err != nil {
		return fmt.Errorf("invalidate journal: %w", err)
	}

	// Truncate WAL, if it exists.
	if _, err := os.Stat(db.WALPath()); err == nil {
		if err := db.TruncateWAL(ctx, 0); err != nil {
			return fmt.Errorf("truncate wal: %w", err)
		}
	}

	pos, err := db.importToLTX(ctx, r)
	if err != nil {
		return err
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
	lockPgno := ltx.LockPgno(hdr.PageSize)
	for pgno := uint32(1); pgno <= hdr.PageN; pgno++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return Pos{}, fmt.Errorf("read page %d: %w", pgno, err)
		}

		// Skip the lock page.
		if pgno == lockPgno {
			continue
		}

		// Reset file change counter & schema cookie to ensure existing connections reload.
		if pgno == 1 {
			binary.BigEndian.PutUint32(buf[24:], 0)
			binary.BigEndian.PutUint32(buf[40:], 0)
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return Pos{}, fmt.Errorf("encode ltx page: pgno=%d err=%w", pgno, err)
		}

		pageChksum := ltx.ChecksumPage(pgno, buf)
		pos.PostApplyChecksum = ltx.ChecksumFlag | (pos.PostApplyChecksum ^ pageChksum)
	}

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

		// There is a race condition where a passive checkpoint can copy out data
		// from the WAL to the database before an LTX file is written. To prevent
		// that, we require that the owner has acquired the WRITE lock before
		// acquiring the CKPT lock or that the WRITE lock is not currently locked.
		// This ensures that no checkpoint can occur while a WAL write is in-progress.
		//
		// Another option would be to rebuild the last LTX file on recovery from the
		// WAL, however there could be a race in there between WAL fsync() and
		// checkpoint. That option needs more investigation.
		if lockType == LockTypeCkpt &&
			db.writeLock.State() != RWMutexStateUnlocked && // is there a writer?
			guardSet.write.State() != RWMutexStateExclusive { // is this owner the writer?
			TraceLog.Printf("[TryLock(%s)]: type=%s owner=%d status=IMPLICIT-FAIL", db.name, lockType, owner)
			return false, nil
		}

		ok := guard.TryLock()

		status := "OK"
		if !ok {
			status = "FAIL"
		}
		TraceLog.Printf("[TryLock(%s)]: type=%s owner=%d status=%s", db.name, lockType, owner, status)

		if !ok {
			return false, nil
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
		if err := db.CommitWAL(ctx); err != nil {
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

// checksum returns the checksum of the database based on per-page checksums.
func (db *DB) checksum(pageN uint32, newWALChecksums map[uint32]uint64) (uint64, error) {
	var chksum uint64
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		pageChksum, ok := db.pageChecksum(pgno, pageN, newWALChecksums)
		if !ok {
			return 0, fmt.Errorf("missing checksum for page %d", pgno)
		}
		chksum = ltx.ChecksumFlag | (chksum ^ pageChksum)
	}
	return chksum, nil
}

// pageChecksum returns the latest checksum for a given page. This first tries
// to find the last valid version of the page on the WAL. If that doesn't exist,
// it returns the checksum of the page on the database. Returns zero if no
// checksum exists for the page.
//
// The lock page will always return a checksum of zero and a true.
//
// Database write lock should be held when invoked.
func (db *DB) pageChecksum(pgno, pageN uint32, newWALChecksums map[uint32]uint64) (chksum uint64, ok bool) {
	// The lock page should never have a checksum.
	if pgno == ltx.LockPgno(db.pageSize) {
		return 0, true
	}

	// Pages past the end of the database should always have no checksum.
	// This is generally handled by the caller but it's added here too.
	if pgno > pageN {
		return 0, false
	}

	// If we're trying to calculate the checksum of an in-progress WAL transaction,
	// we'll check the new checksums to be added first.
	if len(newWALChecksums) > 0 {
		if chksum, ok = newWALChecksums[pgno]; ok {
			return chksum, true
		}
	}

	// Next, find the last valid checksum within committed WAL pages.
	if chksums := db.wal.chksums[pgno]; len(chksums) > 0 {
		return chksums[len(chksums)-1], true
	}

	// Finally, pull the checksum from the database.
	chksum, ok = db.chksums[pgno]
	return chksum, ok
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
	lockPgno := ltx.LockPgno(pageSize)
	var chksum uint64
	for pgno := uint32(1); pgno <= pageN; pgno++ {
		// Skip the lock page.
		if pgno == lockPgno {
			continue
		}

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

// JouralReader represents a reader of the SQLite journal file format.
type JournalReader struct {
	f      *os.File
	fi     os.FileInfo // cached file info
	offset int64       // read offset
	frame  []byte      // frame buffer

	isValid    bool   // true, if at least one valid header exists
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

// IsValid returns true if at least one journal header was read.
func (r *JournalReader) IsValid() bool { return r.isValid }

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

	// Mark journal as valid if we've read at least one header.
	r.isValid = true

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
