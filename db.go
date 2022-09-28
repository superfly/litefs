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
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/superfly/litefs/internal"
	"github.com/superfly/ltx"
)

// DB represents a SQLite database.
type DB struct {
	mu       sync.Mutex
	store    *Store // parent store
	name     string // name of database
	path     string // full on-disk path
	pageSize uint32 // database page size, if known
	pos      Pos    // current tx position

	dirtyPageSet map[uint32]struct{}

	// SQLite locks
	pendingLock  RWMutex
	sharedLock   RWMutex
	reservedLock RWMutex

	// Returns the current time. Used for mocking time in tests.
	Now func() time.Time
}

// NewDB returns a new instance of DB.
func NewDB(store *Store, name string, path string) *DB {
	return &DB{
		store: store,
		name:  name,
		path:  path,

		dirtyPageSet: make(map[uint32]struct{}),

		Now: time.Now,
	}
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
func (db *DB) DatabasePath() string {
	return filepath.Join(db.path, "database")
}

// JournalPath returns the path to the underlying journal file.
func (db *DB) JournalPath() string {
	return filepath.Join(db.path, "journal")
}

// PageSize returns the page size of the underlying database.
func (db *DB) PageSize() uint32 {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.pageSize
}

// Pos returns the current transaction position of the database.
func (db *DB) Pos() Pos {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.pos
}

// setPos sets the current transaction position of the database.
func (db *DB) setPos(pos Pos) error {
	db.pos = pos

	// Invalidate page cache.
	if invalidator := db.store.Invalidator; invalidator != nil {
		if err := invalidator.InvalidatePos(db); err != nil {
			return fmt.Errorf("invalidate pos: %w", err)
		}
	}

	// Update metrics.
	dbTXIDMetricVec.WithLabelValues(db.name).Set(float64(db.pos.TXID))

	return nil
}

// TXID returns the current transaction ID.
func (db *DB) TXID() uint64 { return db.Pos().TXID }

// Open initializes the database from files in its data directory.
func (db *DB) Open() error {
	// Ensure "ltx" directory exists.
	if err := os.MkdirAll(db.LTXDir(), 0777); err != nil {
		return err
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

func (db *DB) recoverFromLTX() error {
	f, err := os.Open(db.LTXDir())
	if err != nil {
		return fmt.Errorf("open ltx dir: %w", err)
	}
	defer func() { _ = f.Close() }()

	fis, err := f.Readdir(-1)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}

	var pos Pos
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
		}
	}

	// Update database to latest position at the end. Skip if no LTX files exist.
	if !pos.IsZero() {
		if err := db.setPos(pos); err != nil {
			return fmt.Errorf("set pos: %w", err)
		}
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

	hdr, err := readSQLiteDatabaseHeader(f)
	if err == io.EOF {
		return nil // no contents yet
	} else if err != nil {
		return fmt.Errorf("cannot read database header: %w", err)
	}
	db.pageSize = hdr.PageSize

	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek to start of database: %w", err)
	}

	// Calculate checksum for entire database.
	chksum, err := ltx.ChecksumReader(f, int(db.pageSize))
	if err != nil {
		return fmt.Errorf("checksum database: %w", err)
	}

	// Ensure database checksum matches checksum in current position.
	if chksum != db.pos.PostApplyChecksum {
		return fmt.Errorf("database checksum (%016x) does not match latest LTX checksum (%016x)", chksum, db.pos.PostApplyChecksum)
	}

	return nil
}

// OpenLTXFile returns a file handle to an LTX file that contains the given TXID.
func (db *DB) OpenLTXFile(txID uint64) (*os.File, error) {
	return os.Open(db.LTXPath(txID, txID))
}

// WriteDatabase writes data to the main database file.
func (db *DB) WriteDatabase(f *os.File, data []byte, offset int64) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Return an error if the current process is not the leader.
	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	} else if len(data) == 0 {
		return nil
	}

	// If we're writing to the database header, ensure WAL mode is not enabled.
	if offset == 0 && !isRollbackJournalEnabled(data) {
		return fmt.Errorf("cannot enable WAL mode with LiteFS")
	}

	// Use page size from the write.
	if db.pageSize == 0 {
		if offset != 0 {
			return fmt.Errorf("cannot determine page size, initial offset (%d) is non-zero", offset)
		}
		hdr, err := readSQLiteDatabaseHeader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("cannot read sqlite database header: %w", err)
		}
		db.pageSize = hdr.PageSize
	}

	// Mark page as dirty.
	pgno := uint32(offset/int64(db.pageSize)) + 1
	db.dirtyPageSet[pgno] = struct{}{}

	// Callback to perform write on handle.
	if _, err := f.WriteAt(data, offset); err != nil {
		return err
	}

	dbDatabaseWriteCountMetricVec.WithLabelValues(db.name).Inc()
	return nil
}

// CreateJournal creates a new journal file on disk.
func (db *DB) CreateJournal() (*os.File, error) {
	if !db.store.IsPrimary() {
		return nil, ErrReadOnlyReplica
	}
	return os.OpenFile(db.JournalPath(), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
}

// WriteJournal writes data to the rollback journal file.
func (db *DB) WriteJournal(f *os.File, data []byte, offset int64) error {
	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	}
	_, err := f.WriteAt(data, offset)
	dbJournalWriteCountMetricVec.WithLabelValues(db.name).Inc()
	return err
}

// CommitJournal deletes the journal file which commits or rolls back the transaction.
func (db *DB) CommitJournal(mode JournalMode) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Return an error if the current process is not the leader.
	if !db.store.IsPrimary() {
		return ErrReadOnlyReplica
	}

	// Read journal header to ensure it's valid.
	if ok, err := db.isJournalHeaderValid(); err != nil {
		return fmt.Errorf("validate journal header: %w", err)
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
	pos := db.pos
	txID := pos.TXID + 1

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

	journalPageMap, err := buildJournalPageMap(journalFile)
	if err != nil {
		return fmt.Errorf("cannot build journal page map: %w", err)
	}

	for _, pageChksum := range journalPageMap {
		postApplyChecksum ^= pageChksum
	}

	// Build sorted list of dirty page numbers.
	pgnos := make([]uint32, 0, len(db.dirtyPageSet))
	for pgno := range db.dirtyPageSet {
		pgnos = append(pgnos, pgno)
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
		PreApplyChecksum: preApplyChecksum,
	}); err != nil {
		return fmt.Errorf("cannot encode ltx header: %s", err)
	}

	// Copy transactions from main database to the LTX file in sorted order.
	buf := make([]byte, db.pageSize)
	for _, pgno := range pgnos {
		// Read page from database.
		offset := int64(pgno-1) * int64(db.pageSize)
		if _, err := dbFile.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("cannot seek to database page: pgno=%d err=%w", pgno, err)
		} else if _, err := io.ReadFull(dbFile, buf); err != nil {
			return fmt.Errorf("cannot read database page: pgno=%d err=%w", pgno, err)
		}

		// Copy page into LTX file.
		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, buf); err != nil {
			return fmt.Errorf("cannot encode ltx page: pgno=%d err=%w", pgno, err)
		}

		// Update rolling checksum.
		postApplyChecksum ^= ltx.ChecksumPage(pgno, buf)
	}

	// Checksum pages removed by truncation.
	if _, err := dbFile.Seek(int64(commit)*int64(db.pageSize), io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to end of database: %w", err)
	}
	for pgno := commit + 1; ; pgno++ {
		if _, err := io.ReadFull(dbFile, buf); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("cannot read truncated database page: pgno=%d err=%w", pgno, err)
		}
		postApplyChecksum ^= ltx.ChecksumPage(pgno, buf)
	}

	// Ensure checksum flag is applied.
	postApplyChecksum |= ltx.ChecksumFlag

	// Calculate checksum for entire database.
	if db.store.StrictVerify {
		if _, err := dbFile.Seek(0, io.SeekStart); err != nil {
			return fmt.Errorf("seek database file: %w", err)
		}

		lr := &io.LimitedReader{R: dbFile, N: int64(commit) * int64(db.pageSize)}

		chksum, err := ltx.ChecksumReader(lr, int(db.pageSize))
		if err != nil {
			return fmt.Errorf("checksum database: %w", err)
		} else if chksum != postApplyChecksum {
			return fmt.Errorf("ltx post-apply checksum mismatch: %016x <> %016x", chksum, postApplyChecksum)
		}
	}

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

	// Notify store of database change.
	db.store.MarkDirty(db.name)

	return nil
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
		return fmt.Errorf("journal mode not implemented: PERSIST")

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
	// Obtain the RESERVED lock, then the SHARED lock, and finally the PENDING lock.
	reservedGuard := db.reservedLock.Guard()
	if err := reservedGuard.Lock(ctx); err != nil {
		return fmt.Errorf("acquire RESERVED write lock: %w", err)
	}
	defer reservedGuard.Unlock()

	sharedGuard := db.sharedLock.Guard()
	if err := sharedGuard.Lock(ctx); err != nil {
		return fmt.Errorf("acquire SHARED write lock: %w", err)
	}
	defer sharedGuard.Unlock()

	pendingGuard := db.pendingLock.Guard()
	if err := pendingGuard.Lock(ctx); err != nil {
		return fmt.Errorf("acquire PENDING write lock: %w", err)
	}
	defer pendingGuard.Unlock()

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

	pageBuf := make([]byte, dec.Header().PageSize)
	for i := 0; ; i++ {
		// Read pgno & page data from LTX file.
		var phdr ltx.PageHeader
		if err := dec.DecodePage(&phdr, pageBuf); err == io.EOF {
			break
		} else if err != nil {
			return fmt.Errorf("decode ltx page[%d]: %w", i, err)
		}

		// Copy to database file.
		offset := int64(phdr.Pgno-1) * int64(dec.Header().PageSize)
		if _, err := dbf.WriteAt(pageBuf, offset); err != nil {
			return fmt.Errorf("write to database file: %w", err)
		}

		// Invalidate page cache.
		if invalidator := db.store.Invalidator; invalidator != nil {
			if err := invalidator.InvalidateDB(db, offset, int64(len(pageBuf))); err != nil {
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

	db.mu.Lock()
	defer db.mu.Unlock()

	// Update transaction for database.
	if err := db.setPos(Pos{
		TXID:              dec.Header().MaxTXID,
		PostApplyChecksum: dec.Trailer().PostApplyChecksum,
	}); err != nil {
		return fmt.Errorf("set pos: %w", err)
	}

	// Notify store of database change.
	db.store.MarkDirty(db.name)

	return nil
}

// GuardSet returns a set of guards that can control locking for a single lock owner.
func (db *DB) GuardSet() *GuardSet {
	return &GuardSet{
		pending:  db.pendingLock.Guard(),
		shared:   db.sharedLock.Guard(),
		reserved: db.reservedLock.Guard(),
	}
}

func (db *DB) PendingLock() *RWMutex  { return &db.pendingLock }
func (db *DB) ReservedLock() *RWMutex { return &db.reservedLock }
func (db *DB) SharedLock() *RWMutex   { return &db.sharedLock }

// InWriteTx returns true if the RESERVED lock has an exclusive lock.
func (db *DB) InWriteTx() bool {
	return db.reservedLock.State() == RWMutexStateExclusive
}

// WriteSnapshotTo writes an LTX snapshot to dst.
func (db *DB) WriteSnapshotTo(ctx context.Context, dst io.Writer) (header ltx.Header, trailer ltx.Trailer, err error) {
	pendingGuard := db.pendingLock.Guard()
	if err := pendingGuard.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire PENDING read lock: %w", err)
	}
	defer pendingGuard.Unlock()

	sharedGuard := db.sharedLock.Guard()
	if err := sharedGuard.RLock(ctx); err != nil {
		return header, trailer, fmt.Errorf("acquire SHARED read lock: %w", err)
	}
	defer sharedGuard.Unlock()
	pendingGuard.Unlock()

	// Determine current position to get TXID.
	db.mu.Lock()
	pos := db.pos
	db.mu.Unlock()

	// Open database file.
	f, err := os.Open(db.DatabasePath())
	if err != nil {
		return header, trailer, fmt.Errorf("open database file: %w", err)
	}
	defer func() { _ = f.Close() }()

	// Read database header and then reset back to the beginning of the file.
	dbHeader, err := readSQLiteDatabaseHeader(f)
	if err != nil {
		return header, trailer, fmt.Errorf("read database header: %w", err)
	} else if _, err := f.Seek(0, io.SeekStart); err != nil {
		return header, trailer, fmt.Errorf("seek database file: %w", err)
	}

	// Write current database state to an LTX writer.
	enc := ltx.NewEncoder(dst)
	if err := enc.EncodeHeader(ltx.Header{
		Version:   ltx.Version,
		PageSize:  dbHeader.PageSize,
		Commit:    dbHeader.PageN,
		MinTXID:   1,
		MaxTXID:   pos.TXID,
		Timestamp: uint64(db.Now().UnixMilli()),
	}); err != nil {
		return header, trailer, fmt.Errorf("encode ltx header: %w", err)
	}

	// Write page frames.
	pageData := make([]byte, dbHeader.PageSize)
	var chksum uint64
	for i := uint32(0); i < dbHeader.PageN; i++ {
		pgno := i + 1

		if _, err := io.ReadFull(f, pageData); err != nil {
			return header, trailer, fmt.Errorf("read database page: %w", err)
		}

		if err := enc.EncodePage(ltx.PageHeader{Pgno: pgno}, pageData); err != nil {
			return header, trailer, fmt.Errorf("encode page frame: %w", err)
		}

		chksum ^= ltx.ChecksumPage(pgno, pageData)
	}

	// Set the database checksum before we write the trailer.
	enc.SetPostApplyChecksum(ltx.ChecksumFlag | chksum)

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
		log.Printf("removing ltx file, per retention: db=%s file=%s", db.Name(), ent.Name())
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
	PageSize uint32 `json:"pageSize"`
	TXID     string `json:"txid"`
	Checksum string `json:"checksum"`

	PendingLock  string `json:"pendingLock"`
	SharedLock   string `json:"sharedLock"`
	ReservedLock string `json:"reservedLock"`
}

func buildJournalPageMap(f *os.File) (map[uint32]uint64, error) {
	// Generate a map of pages and their new checksums.
	m := make(map[uint32]uint64)
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	for i := 0; ; i++ {
		if err := buildJournalPageMapFromSegment(f, m); err == io.EOF {
			return m, nil
		} else if err == errInvalidJournalHeader && i > 0 {
			return m, nil // read at least one segment
		} else if err != nil {
			return nil, fmt.Errorf("journal segment(%d): %w", i, err)
		}
	}
}

// Reads a journal header and subsequent pages.
//
// Returns true if the end-of-file was reached. Function should be called
// continually until the EOF is found as the journal may have multiple sections.
func buildJournalPageMapFromSegment(f *os.File, m map[uint32]uint64) error {
	// Read journal header.
	buf := make([]byte, len(SQLITE_JOURNAL_HEADER_STRING)+20)
	if _, err := io.ReadFull(f, buf); err != nil {
		return errInvalidJournalHeader
	} else if string(buf[:len(SQLITE_JOURNAL_HEADER_STRING)]) != SQLITE_JOURNAL_HEADER_STRING {
		return errInvalidJournalHeader
	}

	// Read fields after header magic.
	hdr := buf[len(SQLITE_JOURNAL_HEADER_STRING):]
	pageN := int32(binary.BigEndian.Uint32(hdr[0:])) // The number of pages in the next segment of the journal, or -1 to mean all content to the end of the file
	//nonce = binary.BigEndian.Uint32(hdr[4:])            // A random nonce for the checksum
	//initialSize = binary.BigEndian.Uint32(hdr[8:])            // Initial size of the database in pages
	sectorSize := binary.BigEndian.Uint32(hdr[12:]) // Initial size of the database in pages
	pageSize := binary.BigEndian.Uint32(hdr[16:])   // Initial size of the database in pages
	if pageSize == 0 {
		return fmt.Errorf("invalid page size in journal header")
	}

	// Move to the end of the sector.
	if _, err := f.Seek(int64(sectorSize)-int64(len(buf)), io.SeekCurrent); err != nil {
		return fmt.Errorf("cannot seek to next sector: %w", err)
	}

	// Read journal entries. Page count may be -1 to read all entries.
	frame := make([]byte, pageSize+4+4)
	for pageN != 0 {
		// Read page number, page data, & checksum.
		if _, err := io.ReadFull(f, frame); err != nil {
			return fmt.Errorf("cannot read journal frame: %w", err)
		}
		pgno := binary.BigEndian.Uint32(frame[0:])
		data := frame[4 : len(frame)-4]

		// TODO: Verify journal checksum

		// Calculate LTX page checksum and add it to the map.
		chksum := ltx.ChecksumPage(pgno, data)
		m[pgno] = chksum

		// Exit after the specified number of pages, if specified in the header.
		if pageN > 0 {
			pageN -= 1
		}
	}

	// Move to next journal header at the next sector.
	if offset, err := f.Seek(0, io.SeekCurrent); err != nil {
		return fmt.Errorf("seek current: %w", err)
	} else if _, err := f.Seek(nextMultipleOf(offset, int64(sectorSize)), io.SeekStart); err != nil {
		return fmt.Errorf("seek to: %w", err)
	}

	return nil
}

// nextMultipleOf returns the next multiple of denom based on v.
// Returns v if it is a multiple of denom.
func nextMultipleOf(v, denom int64) int64 {
	mod := v % denom
	if mod == 0 {
		return v
	}
	return v + (denom - mod)
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

const (
	SQLITE_DATABASE_HEADER_STRING = "SQLite format 3\x00"

	/// Magic header string that identifies a SQLite journal header.
	/// https://www.sqlite.org/fileformat.html#the_rollback_journal
	SQLITE_JOURNAL_HEADER_STRING = "\xd9\xd5\x05\xf9\x20\xa1\x63\xd7"

	// Location of the database size, in pages, in the main database file.
	SQLITE_DATABASE_SIZE_OFFSET = 28
)

var errInvalidJournalHeader = errors.New("invalid journal header")

// LockType represents a SQLite lock type.
type LockType int

const (
	LockTypePending  = 0x40000000
	LockTypeReserved = 0x40000001
	LockTypeShared   = 0x40000002
)

// ParseLockRange returns a list of SQLite locks that are within a range.
func ParseLockRange(start, end uint64) []LockType {
	a := make([]LockType, 0, 3)
	if start <= LockTypePending && LockTypePending <= end {
		a = append(a, LockTypePending)
	}
	if start <= LockTypeReserved && LockTypeReserved <= end {
		a = append(a, LockTypeReserved)
	}
	if start <= LockTypeShared && LockTypeShared <= end {
		a = append(a, LockTypeShared)
	}
	return a
}

// GuardSet represents a set of mutex guards held on database locks by a single owner.
type GuardSet struct {
	pending  RWMutexGuard
	shared   RWMutexGuard
	reserved RWMutexGuard
}

// Guard returns a guard by lock type. Panic on invalid lock type.
func (s *GuardSet) Guard(lockType LockType) *RWMutexGuard {
	switch lockType {
	case LockTypePending:
		return &s.pending
	case LockTypeShared:
		return &s.shared
	case LockTypeReserved:
		return &s.reserved
	default:
		panic("GuardSet.Guard(): invalid lock type")
	}
}

// Unlock unlocks all the guards in reversed order that they are acquired by SQLite.
func (s *GuardSet) Unlock() {
	s.pending.Unlock()
	s.shared.Unlock()
	s.reserved.Unlock()
}

// isRollbackJournalEnabled returns true if the file format read or write
// version is not set to "1" to indicate a rollback journal.
//
// See: https://www.sqlite.org/fileformat.html#the_database_header
func isRollbackJournalEnabled(b []byte) bool {
	if len(b) < databaseHeaderSize {
		return false
	}
	return b[18] == 1 || b[19] == 1
}

// SQLite constants
const (
	databaseHeaderSize = 100
)

type sqliteDBHeader struct {
	WriteVersion int
	ReadVersion  int
	PageSize     uint32
	PageN        uint32
}

// readSQLiteDatabaseHeader reads specific fields from the header of a SQLite database file.
func readSQLiteDatabaseHeader(r io.Reader) (hdr sqliteDBHeader, err error) {
	b := make([]byte, databaseHeaderSize)
	if _, err := io.ReadFull(r, b); err != nil {
		return hdr, err
	} else if !bytes.Equal(b[:len(SQLITE_DATABASE_HEADER_STRING)], []byte(SQLITE_DATABASE_HEADER_STRING)) {
		return hdr, fmt.Errorf("invalid sqlite database file: %x <> %x", b[:len(SQLITE_DATABASE_HEADER_STRING)], SQLITE_DATABASE_HEADER_STRING)
	}

	hdr.WriteVersion = int(b[18])
	hdr.ReadVersion = int(b[19])

	hdr.PageSize = uint32(binary.BigEndian.Uint16(b[16:]))
	if hdr.PageSize == 1 {
		hdr.PageSize = 65536
	}
	if !ltx.IsValidPageSize(hdr.PageSize) {
		return hdr, fmt.Errorf("invalid sqlite page size: %d", hdr.PageSize)
	}

	hdr.PageN = binary.BigEndian.Uint32(b[28:])

	return hdr, nil
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
)
