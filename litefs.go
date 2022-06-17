package litefs

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

func isWALFrameAligned(off int64, pageSize int) bool {
	return off == 0 || ((off-WALHeaderSize)%(WALFrameHeaderSize+int64(pageSize))) == 0
}

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
