package litefs

import (
	"fmt"
	"strconv"
)

// LiteFS errors
var (
	ErrDatabaseNotFound = fmt.Errorf("database not found")
	ErrDatabaseExists   = fmt.Errorf("database already exists")
)

const PageSize = 4096

// SQLite constants
const (
	WALHeaderSize      = 32
	WALFrameHeaderSize = 24
	WALIndexHeaderSize = 136
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

// Open file description lock constants.
const (
	F_OFD_GETLK  = 36
	F_OFD_SETLK  = 37
	F_OFD_SETLKW = 38
)

// Pos represents the transactional position of a database.
type Pos struct {
	TXID uint64
	// Chksum uint64
}

// IsZero returns true if the position is empty.
func (p Pos) IsZero() bool {
	return p == (Pos{})
}

// FormatDBID formats id as a 16-character hex string.
func FormatDBID(id uint64) string {
	return fmt.Sprintf("%016x", id)
}

// ParseDBID parses a 16-character hex string into a database ID.
func ParseDBID(s string) (uint64, error) {
	if len(s) != 16 {
		return 0, fmt.Errorf("invalid formatted database id length: %q", s)
	}
	v, err := strconv.ParseUint(s, 10, 16)
	if err != nil {
		return 0, fmt.Errorf("invalid database id format: %q", s)
	}
	return v, nil
}

func isWALFrameAligned(off int64, pageSize int) bool {
	return off == 0 || ((off-WALHeaderSize)%(WALFrameHeaderSize+int64(pageSize))) == 0
}

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
