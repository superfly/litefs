package litefs

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"

	"github.com/superfly/litefs/internal"
	"github.com/superfly/ltx"
)

// DB represents a SQLite database.
type DB struct {
	mu       sync.Mutex
	store    *Store // parent store
	id       uint64 // database identifier
	name     string // name of database
	path     string // full on-disk path
	pageSize uint32 // database page size, if known
	pos      Pos    // current tx position

	dirtyPageSet map[uint32]struct{}

	// SQLite locks
	locks struct {
		mu       sync.Mutex
		pending  DBLock
		shared   DBLock
		reserved DBLock
	}
}

// NewDB returns a new instance of DB.
func NewDB(store *Store, id uint64, path string) *DB {
	return &DB{
		store: store,
		id:    id,
		path:  path,

		dirtyPageSet: make(map[uint32]struct{}),
	}
}

// ID returns the database ID.
func (db *DB) ID() uint64 { return db.id }

// Name of the database name.
func (db *DB) Name() string { return db.name }

// Path of the database's data directory.
func (db *DB) Path() string { return db.path }

// LTXDir returns the path to the directory of LTX transaction files.
func (db *DB) LTXDir() string { return filepath.Join(db.path, "ltx") }

// LTXPath returns the path of an LTX file.
func (db *DB) LTXPath(minTXID, maxTXID uint64) string {
	return filepath.Join(db.LTXDir(), ltx.FormatFilename(minTXID, maxTXID))
}

// Pos returns the current transaction position of the database.
func (db *DB) Pos() Pos {
	db.mu.Lock()
	defer db.mu.Unlock()
	return db.pos
}

// TXID returns the current transaction ID.
func (db *DB) TXID() uint64 { return db.Pos().TXID }

// Open initializes the database from files in its data directory.
func (db *DB) Open() error {
	// Read name file.
	name, err := os.ReadFile(filepath.Join(db.path, "name"))
	if err != nil {
		return fmt.Errorf("cannot find name file: %w", err)
	}
	db.name = string(name)

	// Ensure "ltx" directory exists.
	if err := os.MkdirAll(db.LTXDir(), 0777); err != nil {
		return err
	}

	if err := db.recoverFromLTX(); err != nil {
		return fmt.Errorf("recover ltx: %w", err)
	}

	return nil
}

func (db *DB) recoverFromLTX() error {
	f, err := os.Open(db.LTXDir())
	if err != nil {
		return fmt.Errorf("open ltx dir: %w", err)
	}
	defer f.Close()

	fis, err := f.Readdir(-1)
	if err != nil {
		return fmt.Errorf("readdir: %w", err)
	}
	for _, fi := range fis {
		_, maxTXID, err := ltx.ParseFilename(fi.Name())
		if err != nil {
			continue
		} else if maxTXID > db.pos.TXID {
			db.pos = Pos{TXID: maxTXID}
		}
	}

	return nil
}

// OpenLTXFile returns a file handle to an LTX file that contains the given TXID.
func (db *DB) OpenLTXFile(txID uint64) (*os.File, error) {
	return os.Open(filepath.Join(db.LTXDir(), ltx.FormatFilename(txID, txID)))
}

// WriteDatabase writes data to the main database file.
func (db *DB) WriteDatabase(f *os.File, data []byte, offset int64) error {
	if len(data) == 0 {
		return nil
	}

	db.mu.Lock()
	defer db.mu.Unlock()

	// Use page size from the write.
	// TODO: Read page size from meta page.
	if db.pageSize == 0 {
		db.pageSize = uint32(len(data))
	}

	// Mark page as dirty.
	pgno := uint32(offset/int64(db.pageSize)) + 1
	db.dirtyPageSet[pgno] = struct{}{}

	// Callback to perform write on handle.
	if _, err := f.WriteAt(data, offset); err != nil {
		return err
	}

	return nil
}

// CreateJournal creates a new journal file on disk.
func (db *DB) CreateJournal() (*os.File, error) {
	return os.OpenFile(filepath.Join(db.path, "journal"), os.O_RDWR|os.O_CREATE|os.O_EXCL|os.O_TRUNC, 0666)
}

// WriteJournal writes data to the rollback journal file.
func (db *DB) WriteJournal(f *os.File, data []byte, offset int64) error {
	_, err := f.WriteAt(data, offset)
	return err
}

// CommitJournal deletes the journal file which commits or rolls back the transaction.
func (db *DB) CommitJournal(mode JournalMode) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// Read journal header to ensure it's valid.
	if ok, err := db.isJournalHeaderValid(); err != nil {
		return err
	} else if !ok {
		return db.invalidateJournal(mode) // rollback
	}
	return db.commitJournal(mode)
}

// isJournalHeaderValid returns true if the journal starts with the journal magic.
func (db *DB) isJournalHeaderValid() (bool, error) {
	f, err := os.Open(filepath.Join(db.path, "journal"))
	if err != nil {
		return false, err
	}
	defer f.Close()

	buf := make([]byte, len(SQLITE_JOURNAL_HEADER_STRING))
	if _, err := io.ReadFull(f, buf); err != nil {
		return false, err
	}
	return string(buf) == SQLITE_JOURNAL_HEADER_STRING, nil
}

// commitJournal creates a new transaction file from the journal and commits.
func (db *DB) commitJournal(mode JournalMode) error {
	if db.pageSize == 0 {
		return fmt.Errorf("unknown page size")
	}

	// Determine transaction ID of the in-process transaction.
	txID := db.pos.TXID + 1

	dbFile, err := os.Open(filepath.Join(db.path, "database"))
	if err != nil {
		return fmt.Errorf("cannot open database file: %w", err)
	}
	defer dbFile.Close()

	var commit uint32
	if _, err := dbFile.Seek(SQLITE_DATABASE_SIZE_OFFSET, io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to database size: %w", err)
	} else if err := binary.Read(dbFile, binary.BigEndian, &commit); err != nil {
		return fmt.Errorf("cannot read database size: %w", err)
	}

	// Compute incremental checksum based off previous LTX database checksum.
	var chksum uint64 // TODO: Read from previous LTX file.

	// Remove page checksums from old pages in the journal.
	journalFile, err := os.Open(filepath.Join(db.path, "journal"))
	if err != nil {
		return fmt.Errorf("cannot open journal file: %w", err)
	}

	journalPageMap, err := buildJournalPageMap(journalFile)
	if err != nil {
		return fmt.Errorf("cannot build journal page map: %w", err)
	}

	for _, pageChksum := range journalPageMap {
		chksum ^= pageChksum
	}

	// Build sorted list of dirty page numbers.
	pgnos := make([]uint32, 0, len(db.dirtyPageSet))
	for pgno := range db.dirtyPageSet {
		pgnos = append(pgnos, pgno)
	}
	sort.Slice(pgnos, func(i, j int) bool { return pgnos[i] < pgnos[j] })

	hdr := ltx.Header{
		Version:  1,
		PageSize: db.pageSize,
		PageN:    uint32(len(pgnos)),
		Commit:   commit,
		DBID:     db.id,
		MinTXID:  txID,
		MaxTXID:  txID,
	}

	// Open file descriptors for the header & page blocks for new LTX file.
	ltxPath := filepath.Join(db.LTXDir(), ltx.FormatFilename(hdr.MinTXID, hdr.MaxTXID))

	hf, err := os.Create(ltxPath)
	if err != nil {
		return fmt.Errorf("cannot create LTX file: %w", err)
	}
	defer hf.Close()

	pf, err := os.OpenFile(ltxPath, os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("cannot open LTX page block for writing: %w", err)
	}
	defer pf.Close()

	if _, err := pf.Seek(hdr.HeaderBlockSize(), io.SeekStart); err != nil {
		return fmt.Errorf("cannot seek to page block: %w", err)
	}

	hw := ltx.NewHeaderBlockWriter(hf)
	if err := hw.WriteHeader(hdr); err != nil {
		return fmt.Errorf("cannot write header: %s", err)
	}
	pw := ltx.NewPageBlockWriter(pf, hdr.PageN, hdr.PageSize)

	// Copy transactions from main database to the LTX file in sorted order.
	buf := make([]byte, db.pageSize)
	for _, pgno := range pgnos {
		offset := int64(pgno-1) * int64(db.pageSize)
		if _, err := dbFile.Seek(offset, io.SeekStart); err != nil {
			return fmt.Errorf("cannot seek to database page: pgno=%d err=%w", pgno, err)
		} else if _, err := io.ReadFull(dbFile, buf); err != nil {
			return fmt.Errorf("cannot read database page: pgno=%d err=%w", pgno, err)
		}

		// Write header info.
		if err := hw.WritePageHeader(ltx.PageHeader{Pgno: pgno}); err != nil {
			return fmt.Errorf("cannot write page header: pgno=%d err=%w", pgno, err)
		} else if _, err := pw.Write(buf); err != nil {
			return fmt.Errorf("cannot write page data: pgno=%d err=%w", pgno, err)
		}

		// Update incremental checksum.
		chksum ^= ltx.ChecksumPage(pgno, buf)
	}

	// TODO: Write event data to LTX file.

	// Finish page block to compute checksum and then finish header block.
	hw.SetPageBlockChecksum(pw.Checksum())
	if err := pw.Close(); err != nil {
		return fmt.Errorf("close page block writer: %s", err)
	} else if err := hw.Close(); err != nil {
		return fmt.Errorf("close header block writer: %s", err)
	}

	// Ensure file is persisted to disk.
	if err := dbFile.Sync(); err != nil {
		return fmt.Errorf("cannot sync ltx file: %w", err)
	}

	if err := db.invalidateJournal(mode); err != nil {
		return fmt.Errorf("invalidate journal: %w", err)
	}

	// Update transaction for database.
	db.pos = Pos{TXID: txID}

	// Notify store of database change.
	db.store.MarkDirty(db.id)

	return nil
}

// invalidateJournal invalidates the journal file based on the journal mode.
func (db *DB) invalidateJournal(mode JournalMode) error {
	journalPath := filepath.Join(db.path, "journal")

	switch mode {
	case JournalModeDelete:
		if err := os.Remove(journalPath); err != nil {
			return fmt.Errorf("remove journal file: %w", err)
		}

	case JournalModeTruncate:
		if err := os.Truncate(journalPath, 0); err != nil {
			return fmt.Errorf("truncate: %w", err)
		} else if err := internal.Sync(journalPath); err != nil {
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

// TryApplyLTX attempts to apply an LTX file to the database.
func (db *DB) TryApplyLTX(path string) error {
	db.mu.Lock()
	defer db.mu.Unlock()

	// TODO: Obtain RESERVED lock.

	// Open database file for writing.
	dbf, err := os.OpenFile(filepath.Join(db.path, "database"), os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open database file: %w", err)
	}
	defer dbf.Close()

	// Open LTX header reader.
	hf, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer hf.Close()

	var hdr ltx.Header
	hr := ltx.NewHeaderBlockReader(hf)
	if err := hr.ReadHeader(&hdr); err != nil {
		return fmt.Errorf("read header: %s", err)
	}

	// Open page block reader.
	pf, err := os.Open(path)
	if err != nil {
		return fmt.Errorf("open file: %w", err)
	}
	defer pf.Close()

	if _, err := pf.Seek(int64(hdr.HeaderBlockSize()), io.SeekStart); err != nil {
		return fmt.Errorf("seek to page block: %w", err)
	}

	pr := ltx.NewPageBlockReader(pf, hdr.PageN, hdr.PageSize, hdr.PageBlockChecksum)
	pageBuf := make([]byte, hdr.PageSize)
	for i := uint32(0); i < hdr.PageN; i++ {
		// Read pgno & page data from LTX file.
		var phdr ltx.PageHeader
		if err := hr.ReadPageHeader(&phdr); err != nil {
			return fmt.Errorf("read page header[%d]: %w", i, err)
		} else if _, err := io.ReadFull(pr, pageBuf); err != nil {
			return fmt.Errorf("read page data[%d]: %w", i, err)
		}

		// Copy to database file.
		offset := int64(phdr.Pgno-1) * int64(hdr.PageSize)
		if _, err := dbf.WriteAt(pageBuf, offset); err != nil {
			return fmt.Errorf("write to database file: %w", err)
		}

		// Invalidate page cache.
		if notifier := db.store.InodeNotifier; notifier != nil {
			if err := notifier.InodeNotify(db.ID(), offset, int64(hdr.PageSize)); err != nil {
				return fmt.Errorf("inode notify: %w", err)
			}
		}
	}

	// Truncate database file to size after LTX file.
	if err := dbf.Truncate(int64(hdr.Commit) * int64(hdr.PageSize)); err != nil {
		return fmt.Errorf("truncate database file: %w", err)
	}

	// Sync changes to disk.
	if err := dbf.Sync(); err != nil {
		return fmt.Errorf("sync database file: %w", err)
	}

	// Update transaction for database.
	db.pos = Pos{TXID: hdr.MaxTXID}

	// Notify store of database change.
	db.store.MarkDirty(db.id)

	return nil
}

// WithLocksMutex executes fn with the SQLite lock set mutex held.
func (db *DB) WithLocksMutex(fn func()) {
	db.locks.mu.Lock()
	defer db.locks.mu.Unlock()
	fn()
}

// PendingLock returns a reference to the PENDING lock object.
func (db *DB) PendingLock() *DBLock { return &db.locks.pending }

// ReservedLock returns a reference to the RESERVED lock object.
func (db *DB) ReservedLock() *DBLock { return &db.locks.reserved }

// SharedLock returns a reference to the SHARED lock object.
func (db *DB) SharedLock() *DBLock { return &db.locks.shared }

// InWriteTx returns true if the RESERVED lock has an exclusive lock.
func (db *DB) InWriteTx() bool {
	db.locks.mu.Lock()
	defer db.locks.mu.Unlock()
	return db.locks.reserved.Excl
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

/// Reads a journal header and subsequent pages.
///
/// Returns true if the end-of-file was reached. Function should be called
/// continually until the EOF is found as the journal may have multiple sections.
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

// DBLock represents a file lock on the database.
type DBLock struct {
	SharedN int  // number of shared locks
	Excl    bool // if true, exclusive lock held
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
	/// Magic header string that identifies a SQLite journal header.
	/// https://www.sqlite.org/fileformat.html#the_rollback_journal
	SQLITE_JOURNAL_HEADER_STRING = "\xd9\xd5\x05\xf9\x20\xa1\x63\xd7"

	// Location of the database size, in pages, in the main database file.
	SQLITE_DATABASE_SIZE_OFFSET = 28
)

var errInvalidJournalHeader = errors.New("invalid journal header")
