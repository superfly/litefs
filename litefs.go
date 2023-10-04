package litefs

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"unsafe"

	"golang.org/x/exp/slog"
)

func init() {
	assert(unsafe.Sizeof(walIndexHdr{}) == 48, "invalid walIndexHdr size")
	assert(unsafe.Sizeof(walCkptInfo{}) == 40, "invalid walCkptInfo size")
}

// Global log level. Can be adjusted dynamically.
var LogLevel struct {
	sync.Mutex
	slog.LevelVar
}

const (
	// ClusterIDLen is the length of a cluster ID.
	ClusterIDLen = 20

	// ClusterIDPrefix is the prefix for every cluster ID.
	ClusterIDPrefix = "LFSC"
)

// ErrInvalidClusterID is returned when a cluster ID is invalid.
var ErrInvalidClusterID = errors.New("invalid cluster id")

// GenerateClusterID returns a new, randomly-generated cluster ID.
func GenerateClusterID() string {
	b := make([]byte, (ClusterIDLen-len(ClusterIDPrefix))/2)
	if _, err := io.ReadFull(rand.Reader, b); err != nil {
		panic(err)
	}
	return fmt.Sprintf("%s%X", ClusterIDPrefix, b)
}

// ValidateClusterID returns nil if id is a valid cluster ID.
func ValidateClusterID(id string) error {
	if len(id) != ClusterIDLen || !strings.HasPrefix(id, ClusterIDPrefix) {
		return ErrInvalidClusterID
	}
	return nil
}

// NodeInfo represents basic info about a node.
type NodeInfo struct {
	ClusterID string `json:"clusterID,omitempty"` // cluster ID
	IsPrimary bool   `json:"isPrimary"`           // if true, node is currently primary
	Candidate bool   `json:"candidate"`           // if true, node is eligible to be primary
	Path      string `json:"path"`                // data directory

	Primary struct {
		Hostname string `json:"hostname"`
	} `json:"primary"`
}

// Environment represents an interface for interacting with the host environment.
type Environment interface {
	// Type returns the name of the environment type.
	Type() string

	// SetPrimaryStatus sets marks the current node as the primary or not.
	SetPrimaryStatus(ctx context.Context, isPrimary bool)
}

type nopEnvironment struct{}

func (*nopEnvironment) Type() string { return "" }

func (*nopEnvironment) SetPrimaryStatus(ctx context.Context, v bool) {}

// NativeEndian is always set to little endian as that is the only endianness
// used by supported platforms for LiteFS. This may be expanded in the future.
var NativeEndian = binary.LittleEndian

// LiteFS errors
var (
	ErrDatabaseNotFound = fmt.Errorf("database not found")
	ErrDatabaseExists   = fmt.Errorf("database already exists")

	ErrNoPrimary     = errors.New("no primary")
	ErrPrimaryExists = errors.New("primary exists")
	ErrNotEligible   = errors.New("not eligible to become primary")
	ErrLeaseExpired  = errors.New("lease expired")
	ErrNoHaltPrimary = errors.New("no remote halt needed on primary node")

	ErrReadOnlyReplica  = fmt.Errorf("read only replica")
	ErrDuplicateLTXFile = fmt.Errorf("duplicate ltx file")
)

// SQLite constants
const (
	WALHeaderSize      = 32
	WALFrameHeaderSize = 24
	WALIndexHeaderSize = 136
	WALIndexBlockSize  = 32768
)

// SQLite rollback journal lock constants.
const (
	PENDING_BYTE  = 0x40000000
	RESERVED_BYTE = (PENDING_BYTE + 1)
	SHARED_FIRST  = (PENDING_BYTE + 2)
	SHARED_SIZE   = 510
)

// SQLite WAL lock constants.
const (
	WAL_WRITE_LOCK   = 120
	WAL_CKPT_LOCK    = 121
	WAL_RECOVER_LOCK = 122
	WAL_READ_LOCK0   = 123
	WAL_READ_LOCK1   = 124
	WAL_READ_LOCK2   = 125
	WAL_READ_LOCK3   = 126
	WAL_READ_LOCK4   = 127
)

// JournalMode represents a SQLite journal mode.
type JournalMode string

const (
	JournalModeDelete   = "DELETE"
	JournalModeTruncate = "TRUNCATE"
	JournalModePersist  = "PERSIST"
	JournalModeWAL      = "WAL"
)

// FileType represents a type of SQLite file.
type FileType int

// Database file types.
const (
	FileTypeNone = FileType(iota)
	FileTypeDatabase
	FileTypeJournal
	FileTypeWAL
	FileTypeSHM
	FileTypePos
	FileTypeLock
)

// IsValid returns true if t is a valid file type.
func (t FileType) IsValid() bool {
	switch t {
	case FileTypeDatabase, FileTypeJournal, FileTypeWAL, FileTypeSHM, FileTypePos, FileTypeLock:
		return true
	default:
		return false
	}
}

// TraceLogFlags are the flags to be used with TraceLog.
const TraceLogFlags = log.LstdFlags | log.Lmicroseconds | log.LUTC

// TraceLog is a log for low-level tracing.
var TraceLog = log.New(io.Discard, "", TraceLogFlags)

var ErrInvalidNodeID = errors.New("invalid node id")

// ParseNodeID parses a 16-character hex string into a node identifier.
func ParseNodeID(s string) (uint64, error) {
	v, err := strconv.ParseUint(s, 16, 64)
	if err != nil {
		return 0, ErrInvalidNodeID
	}
	return v, nil
}

// FormatNodeID formats a node identifier as a 16-character uppercase hex string.
func FormatNodeID(id uint64) string {
	return fmt.Sprintf("%016X", id)
}

// Invalidator is a callback for the store to use to invalidate the kernel page cache.
type Invalidator interface {
	InvalidateDB(db *DB) error
	InvalidateDBRange(db *DB, offset, size int64) error
	InvalidateSHM(db *DB) error
	InvalidatePos(db *DB) error
	InvalidateEntry(name string) error
	InvalidateLag() error
}

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}

// WALReader wraps an io.Reader and parses SQLite WAL frames.
//
// This reader verifies the salt & checksum integrity while it reads. It does
// not enforce transaction boundaries (i.e. it may return uncommitted frames).
// It is the responsibility of the caller to handle this.
type WALReader struct {
	r      io.Reader
	frameN int

	bo       binary.ByteOrder
	pageSize uint32
	seq      uint32

	salt1, salt2     uint32
	chksum1, chksum2 uint32
}

// NewWALReader returns a new instance of WALReader.
func NewWALReader(r io.Reader) *WALReader {
	return &WALReader{r: r}
}

// PageSize returns the page size from the header. Must call ReadHeader() first.
func (r *WALReader) PageSize() uint32 { return r.pageSize }

// Offset returns the file offset of the last read frame.
// Returns zero if no frames have been read.
func (r *WALReader) Offset() int64 {
	if r.frameN == 0 {
		return 0
	}
	return WALHeaderSize + ((int64(r.frameN) - 1) * (WALFrameHeaderSize + int64(r.pageSize)))
}

// ReadHeader reads the WAL header into the reader. Returns io.EOF if WAL is invalid.
func (r *WALReader) ReadHeader() error {
	// If we have a partial WAL, then mark WAL as done.
	hdr := make([]byte, WALHeaderSize)
	if _, err := io.ReadFull(r.r, hdr); err == io.ErrUnexpectedEOF {
		return io.EOF
	} else if err != nil {
		return err
	}

	// Determine byte order of checksums.
	switch magic := binary.BigEndian.Uint32(hdr[0:]); magic {
	case 0x377f0682:
		r.bo = binary.LittleEndian
	case 0x377f0683:
		r.bo = binary.BigEndian
	default:
		return fmt.Errorf("invalid wal header magic: %x", magic)
	}

	// If the header checksum doesn't match then we may have failed with
	// a partial WAL header write during checkpointing.
	chksum1 := binary.BigEndian.Uint32(hdr[24:])
	chksum2 := binary.BigEndian.Uint32(hdr[28:])
	if v0, v1 := WALChecksum(r.bo, 0, 0, hdr[:24]); v0 != chksum1 || v1 != chksum2 {
		return io.EOF
	}

	// Verify version is correct.
	if version := binary.BigEndian.Uint32(hdr[4:]); version != 3007000 {
		return fmt.Errorf("unsupported wal version: %d", version)
	}

	r.pageSize = binary.BigEndian.Uint32(hdr[8:])
	r.seq = binary.BigEndian.Uint32(hdr[12:])
	r.salt1 = binary.BigEndian.Uint32(hdr[16:])
	r.salt2 = binary.BigEndian.Uint32(hdr[20:])
	r.chksum1, r.chksum2 = chksum1, chksum2

	return nil
}

// ReadFrame reads the next frame from the WAL and returns the page number.
// Returns io.EOF at the end of the valid WAL.
func (r *WALReader) ReadFrame(data []byte) (pgno, commit uint32, err error) {
	if len(data) != int(r.pageSize) {
		return 0, 0, fmt.Errorf("WALReader.ReadFrame(): buffer size (%d) must match page size (%d)", len(data), r.pageSize)
	}

	// Read WAL frame header.
	hdr := make([]byte, WALFrameHeaderSize)
	if _, err := io.ReadFull(r.r, hdr); err == io.ErrUnexpectedEOF {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	// Read WAL page data.
	if _, err := io.ReadFull(r.r, data); err == io.ErrUnexpectedEOF {
		return 0, 0, io.EOF
	} else if err != nil {
		return 0, 0, err
	}

	// Verify salt matches the salt in the header.
	salt1 := binary.BigEndian.Uint32(hdr[8:])
	salt2 := binary.BigEndian.Uint32(hdr[12:])
	if r.salt1 != salt1 || r.salt2 != salt2 {
		return 0, 0, io.EOF
	}

	// Verify the checksum is valid.
	chksum1 := binary.BigEndian.Uint32(hdr[16:])
	chksum2 := binary.BigEndian.Uint32(hdr[20:])
	r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, hdr[:8]) // frame header
	r.chksum1, r.chksum2 = WALChecksum(r.bo, r.chksum1, r.chksum2, data)    // frame data
	if r.chksum1 != chksum1 || r.chksum2 != chksum2 {
		return 0, 0, io.EOF
	}

	pgno = binary.BigEndian.Uint32(hdr[0:])
	commit = binary.BigEndian.Uint32(hdr[4:])

	r.frameN++

	return pgno, commit, nil
}

// WALChecksum computes a running SQLite WAL checksum over a byte slice.
func WALChecksum(bo binary.ByteOrder, s0, s1 uint32, b []byte) (uint32, uint32) {
	assert(len(b)%8 == 0, "misaligned checksum byte slice")

	// Iterate over 8-byte units and compute checksum.
	for i := 0; i < len(b); i += 8 {
		s0 += bo.Uint32(b[i:]) + s1
		s1 += bo.Uint32(b[i+4:]) + s0
	}
	return s0, s1
}

// JournalChecksum returns the checksum used by the journal format.
func JournalChecksum(data []byte, initial uint32) uint32 {
	chksum := initial
	for i := len(data) - 200; i > 0; i -= 200 {
		chksum += uint32(data[i])
	}
	return chksum
}

const (
	SQLITE_DATABASE_HEADER_STRING = "SQLite format 3\x00"

	// Location of the database size, in pages, in the main database file.
	SQLITE_DATABASE_SIZE_OFFSET = 28

	/// Magic header string that identifies a SQLite journal header.
	/// https://www.sqlite.org/fileformat.html#the_rollback_journal
	SQLITE_JOURNAL_HEADER_STRING = "\xd9\xd5\x05\xf9\x20\xa1\x63\xd7"

	// Size of the journal header, in bytes.
	SQLITE_JOURNAL_HEADER_SIZE = 28
)

// LockType represents a SQLite lock type.
type LockType int

// String returns the name of the lock type.
func (t LockType) String() string {
	switch t {
	case LockTypeHalt:
		return "HALT"
	case LockTypePending:
		return "PENDING"
	case LockTypeReserved:
		return "RESERVED"
	case LockTypeShared:
		return "SHARED"
	case LockTypeWrite:
		return "WRITE"
	case LockTypeCkpt:
		return "CKPT"
	case LockTypeRecover:
		return "RECOVER"
	case LockTypeRead0:
		return "READ0"
	case LockTypeRead1:
		return "READ1"
	case LockTypeRead2:
		return "READ2"
	case LockTypeRead3:
		return "READ3"
	case LockTypeRead4:
		return "READ4"
	case LockTypeDMS:
		return "DMS"
	default:
		return fmt.Sprintf("UNKNOWN<%d>", t)
	}
}

const (
	// Database file locks
	LockTypeHalt     = LockType(72)         // LiteFS-specific lock byte
	LockTypePending  = LockType(0x40000000) // 1073741824
	LockTypeReserved = LockType(0x40000001) // 1073741825
	LockTypeShared   = LockType(0x40000002) // 1073741826

	// SHM file locks
	LockTypeWrite   = LockType(120)
	LockTypeCkpt    = LockType(121)
	LockTypeRecover = LockType(122)
	LockTypeRead0   = LockType(123)
	LockTypeRead1   = LockType(124)
	LockTypeRead2   = LockType(125)
	LockTypeRead3   = LockType(126)
	LockTypeRead4   = LockType(127)
	LockTypeDMS     = LockType(128)
)

// ContainsLockType returns true if a contains typ.
func ContainsLockType(a []LockType, typ LockType) bool {
	for _, v := range a {
		if v == typ {
			return true
		}
	}
	return false
}

// ParseDatabaseLockRange returns a list of SQLite database locks that are within a range.
//
// This does not include the HALT lock as that is specific to LiteFS and we don't
// want to accidentally include it when locking/unlocking the whole file.
func ParseDatabaseLockRange(start, end uint64) []LockType {
	a := make([]LockType, 0, 3)
	if start <= uint64(LockTypePending) && uint64(LockTypePending) <= end {
		a = append(a, LockTypePending)
	}
	if start <= uint64(LockTypeReserved) && uint64(LockTypeReserved) <= end {
		a = append(a, LockTypeReserved)
	}
	if start <= uint64(LockTypeShared) && uint64(LockTypeShared) <= end {
		a = append(a, LockTypeShared)
	}
	return a
}

// ParseSHMLockRange returns a list of SQLite WAL locks that are within a range.
func ParseSHMLockRange(start, end uint64) []LockType {
	a := make([]LockType, 0, 3)
	if start <= uint64(LockTypeWrite) && uint64(LockTypeWrite) <= end {
		a = append(a, LockTypeWrite)
	}
	if start <= uint64(LockTypeCkpt) && uint64(LockTypeCkpt) <= end {
		a = append(a, LockTypeCkpt)
	}
	if start <= uint64(LockTypeRecover) && uint64(LockTypeRecover) <= end {
		a = append(a, LockTypeRecover)
	}

	if start <= uint64(LockTypeRead0) && uint64(LockTypeRead0) <= end {
		a = append(a, LockTypeRead0)
	}
	if start <= uint64(LockTypeRead1) && uint64(LockTypeRead1) <= end {
		a = append(a, LockTypeRead1)
	}
	if start <= uint64(LockTypeRead2) && uint64(LockTypeRead2) <= end {
		a = append(a, LockTypeRead2)
	}
	if start <= uint64(LockTypeRead3) && uint64(LockTypeRead3) <= end {
		a = append(a, LockTypeRead3)
	}
	if start <= uint64(LockTypeRead4) && uint64(LockTypeRead4) <= end {
		a = append(a, LockTypeRead4)
	}
	if start <= uint64(LockTypeDMS) && uint64(LockTypeDMS) <= end {
		a = append(a, LockTypeDMS)
	}

	return a
}

// GuardSet represents a set of mutex guards by a single owner.
type GuardSet struct {
	owner uint64

	// Database file locks
	pending  RWMutexGuard
	shared   RWMutexGuard
	reserved RWMutexGuard

	// SHM file locks
	write   RWMutexGuard
	ckpt    RWMutexGuard
	recover RWMutexGuard
	read0   RWMutexGuard
	read1   RWMutexGuard
	read2   RWMutexGuard
	read3   RWMutexGuard
	read4   RWMutexGuard
	dms     RWMutexGuard
}

// Pending returns a reference to the PENDING mutex guard.
func (s *GuardSet) Pending() *RWMutexGuard { return &s.pending }

// Shared returns a reference to the SHARED mutex guard.
func (s *GuardSet) Shared() *RWMutexGuard { return &s.shared }

// Reserved returns a reference to the RESERVED mutex guard.
func (s *GuardSet) Reserved() *RWMutexGuard { return &s.reserved }

// Write returns a reference to the WRITE mutex guard.
func (s *GuardSet) Write() *RWMutexGuard { return &s.write }

// Ckpt returns a reference to the CKPT mutex guard.
func (s *GuardSet) Ckpt() *RWMutexGuard { return &s.ckpt }

// Recover returns a reference to the RECOVER mutex guard.
func (s *GuardSet) Recover() *RWMutexGuard { return &s.recover }

// Read0 returns a reference to the READ0 mutex guard.
func (s *GuardSet) Read0() *RWMutexGuard { return &s.read0 }

// Read1 returns a reference to the READ1 mutex guard.
func (s *GuardSet) Read1() *RWMutexGuard { return &s.read1 }

// Read2 returns a reference to the READ2 mutex guard.
func (s *GuardSet) Read2() *RWMutexGuard { return &s.read2 }

// Read3 returns a reference to the READ3 mutex guard.
func (s *GuardSet) Read3() *RWMutexGuard { return &s.read3 }

// Read4 returns a reference to the READ4 mutex guard.
func (s *GuardSet) Read4() *RWMutexGuard { return &s.read4 }

// DMS returns a reference to the DMS mutex guard.
func (s *GuardSet) DMS() *RWMutexGuard { return &s.dms }

// Guard returns a guard by lock type. Panic on invalid lock type.
func (s *GuardSet) Guard(lockType LockType) *RWMutexGuard {
	switch lockType {

	// Database locks
	case LockTypePending:
		return &s.pending
	case LockTypeShared:
		return &s.shared
	case LockTypeReserved:
		return &s.reserved

	// SHM file locks
	case LockTypeWrite:
		return &s.write
	case LockTypeCkpt:
		return &s.ckpt
	case LockTypeRecover:
		return &s.recover
	case LockTypeRead0:
		return &s.read0
	case LockTypeRead1:
		return &s.read1
	case LockTypeRead2:
		return &s.read2
	case LockTypeRead3:
		return &s.read3
	case LockTypeRead4:
		return &s.read4
	case LockTypeDMS:
		return &s.dms

	default:
		panic("GuardSet.Guard(): invalid database lock type")
	}
}

// Unlock unlocks all the guards in reversed order that they are acquired by SQLite.
func (s *GuardSet) Unlock() {
	s.UnlockDatabase()
	s.UnlockSHM()
}

// UnlockDatabase unlocks all the database file guards.
func (s *GuardSet) UnlockDatabase() {
	s.pending.Unlock()
	s.shared.Unlock()
	s.reserved.Unlock()
}

// UnlockSHM unlocks all the SHM file guards.
func (s *GuardSet) UnlockSHM() {
	s.write.Unlock()
	s.ckpt.Unlock()
	s.recover.Unlock()
	s.read0.Unlock()
	s.read1.Unlock()
	s.read2.Unlock()
	s.read3.Unlock()
	s.read4.Unlock()
	s.dms.Unlock()
}

// SQLite constants
const (
	databaseHeaderSize = 100
)

type sqliteDatabaseHeader struct {
	WriteVersion int
	ReadVersion  int
	PageSize     uint32
	PageN        uint32
}

var errInvalidDatabaseHeader = errors.New("invalid database header")

// readSQLiteDatabaseHeader reads specific fields from the header of a SQLite database file.
// Returns errInvalidDatabaseHeader if the database file is too short or if the
// file has invalid magic.
func readSQLiteDatabaseHeader(r io.Reader) (hdr sqliteDatabaseHeader, data []byte, err error) {
	b := make([]byte, databaseHeaderSize)
	if n, err := io.ReadFull(r, b); err == io.ErrUnexpectedEOF {
		return hdr, b[:n], errInvalidDatabaseHeader // short file
	} else if err != nil {
		return hdr, b[:n], err // empty file (EOF) or other errors
	} else if !bytes.Equal(b[:len(SQLITE_DATABASE_HEADER_STRING)], []byte(SQLITE_DATABASE_HEADER_STRING)) {
		return hdr, b[:n], errInvalidDatabaseHeader // invalid magic
	}

	hdr.WriteVersion = int(b[18])
	hdr.ReadVersion = int(b[19])
	hdr.PageSize = uint32(binary.BigEndian.Uint16(b[16:]))
	hdr.PageN = binary.BigEndian.Uint32(b[28:])

	// SQLite page size has a special value for 64K pages.
	if hdr.PageSize == 1 {
		hdr.PageSize = 65536
	}

	return hdr, b, nil
}

// encodePageSize returns sz as a uint16. If sz is 64K, it returns 1.
func encodePageSize(sz uint32) uint16 {
	if sz == 65536 {
		return 1
	}
	return uint16(sz)
}

// DBMode represents either a rollback journal or WAL mode.
type DBMode int

// String returns the string representation of m.
func (m DBMode) String() string {
	switch m {
	case DBModeRollback:
		return "ROLLBACK_MODE"
	case DBModeWAL:
		return "WAL_MODE"
	default:
		return fmt.Sprintf("DBMode<%d>", m)
	}
}

// Database journal modes.
const (
	DBModeRollback = DBMode(0)
	DBModeWAL      = DBMode(1)
)

// walIndexHdr is copied from wal.c
type walIndexHdr struct {
	version     uint32    // Wal-index version
	unused      uint32    // Unused (padding) field
	change      uint32    // Counter incremented each transaction
	isInit      uint8     // 1 when initialized
	bigEndCksum uint8     // True if checksums in WAL are big-endian
	pageSize    uint16    // Database page size in bytes. 1==64K
	mxFrame     uint32    // Index of last valid frame in the WAL
	pageN       uint32    // Size of database in pages
	frameCksum  [2]uint32 // Checksum of last frame in log
	salt        [2]uint32 // Two salt values copied from WAL header
	cksum       [2]uint32 // Checksum over all prior fields
}

// walCkptInfo is copied from wal.c
type walCkptInfo struct {
	backfill          uint32    // Number of WAL frames backfilled into DB
	readMark          [5]uint32 // Reader marks
	lock              [8]uint8  // Reserved space for locks
	backfillAttempted uint32    // WAL frames perhaps written, or maybe not
	notUsed0          uint32    // Available for future enhancements
}

// OS represents an interface for os package calls so they can be mocked for testing.
type OS interface {
	Create(op, name string) (*os.File, error)
	Mkdir(op, path string, perm os.FileMode) error
	MkdirAll(op, path string, perm os.FileMode) error
	Open(op, name string) (*os.File, error)
	OpenFile(op, opname string, flag int, perm os.FileMode) (*os.File, error)
	ReadDir(op, opname string) ([]os.DirEntry, error)
	ReadFile(op, name string) ([]byte, error)
	Remove(op, name string) error
	RemoveAll(op, name string) error
	Rename(op, oldpath, newpath string) error
	Stat(op, name string) (os.FileInfo, error)
	Truncate(op, name string, size int64) error
	WriteFile(op, name string, data []byte, perm os.FileMode) error
}
