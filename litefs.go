package litefs

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// LiteFS errors
var (
	ErrDatabaseNotFound = fmt.Errorf("database not found")
	ErrDatabaseExists   = fmt.Errorf("database already exists")

	ErrNoPrimary     = errors.New("no primary")
	ErrPrimaryExists = errors.New("primary exists")
	ErrLeaseExpired  = errors.New("lease expired")

	ErrReadOnlyReplica = fmt.Errorf("read only replica")
)

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
)

// IsValid returns true if t is a valid file type.
func (t FileType) IsValid() bool {
	switch t {
	case FileTypeDatabase, FileTypeJournal, FileTypeWAL, FileTypeSHM, FileTypePos:
		return true
	default:
		return false
	}
}

// Pos represents the transactional position of a database.
type Pos struct {
	TXID              uint64
	PostApplyChecksum uint64
}

// IsZero returns true if the position is empty.
func (p Pos) IsZero() bool {
	return p == (Pos{})
}

// Client represents a client for connecting to other LiteFS nodes.
type Client interface {
	// Stream starts a long-running connection to stream changes from another node.
	Stream(ctx context.Context, rawurl string, id string, posMap map[uint32]Pos) (io.ReadCloser, error)
}

type StreamFrameType uint32

const (
	StreamFrameTypeDB  = StreamFrameType(1)
	StreamFrameTypeLTX = StreamFrameType(2)
)

type StreamFrame interface {
	io.ReaderFrom
	io.WriterTo
	Type() StreamFrameType
}

// ReadStreamFrame reads a the stream type & frame from the reader.
func ReadStreamFrame(r io.Reader) (StreamFrame, error) {
	var typ StreamFrameType
	if err := binary.Read(r, binary.BigEndian, &typ); err != nil {
		return nil, err
	}

	var f StreamFrame
	switch typ {
	case StreamFrameTypeDB:
		f = &DBStreamFrame{}
	case StreamFrameTypeLTX:
		f = &LTXStreamFrame{}
	default:
		return nil, fmt.Errorf("invalid stream frame type: 0x%02x", typ)
	}

	if _, err := f.ReadFrom(r); err == io.EOF {
		return nil, io.ErrUnexpectedEOF
	} else if err != nil {
		return nil, err
	}
	return f, nil
}

// WriteStreamFrame writes the stream type & frame to the writer.
func WriteStreamFrame(w io.Writer, f StreamFrame) error {
	if err := binary.Write(w, binary.BigEndian, f.Type()); err != nil {
		return err
	}
	_, err := f.WriteTo(w)
	return err
}

// DBStreamFrame represents a frame with basic database information.
// This is sent at the beginning of the stream and when a new database is created.
type DBStreamFrame struct {
	DBID uint32
	Name string
}

// Type returns the type of stream frame.
func (*DBStreamFrame) Type() StreamFrameType { return StreamFrameTypeDB }

func (f *DBStreamFrame) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &f.DBID); err != nil {
		return 0, err
	}

	var nameN uint32
	if err := binary.Read(r, binary.BigEndian, &nameN); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}

	name := make([]byte, nameN)
	if _, err := io.ReadFull(r, name); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}
	f.Name = string(name)

	return 0, nil
}

func (f *DBStreamFrame) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, f.DBID); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(f.Name))); err != nil {
		return 0, err
	} else if _, err := w.Write([]byte(f.Name)); err != nil {
		return 0, err
	}
	return 0, nil
}

type LTXStreamFrame struct {
}

// Type returns the type of stream frame.
func (*LTXStreamFrame) Type() StreamFrameType { return StreamFrameTypeLTX }

func (f *LTXStreamFrame) ReadFrom(r io.Reader) (int64, error) {
	return 0, nil
}

func (f *LTXStreamFrame) WriteTo(w io.Writer) (int64, error) {
	return 0, nil
}

// Invalidator is a callback for the store to use to invalidate the kernel page cache.
type Invalidator interface {
	InvalidateDB(db *DB, offset, size int64) error
	InvalidatePos(db *DB) error
}

func assert(condition bool, msg string) {
	if !condition {
		panic("assertion failed: " + msg)
	}
}
