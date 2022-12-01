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

// String returns a string representation of the position.
func (p Pos) String() string {
	return fmt.Sprintf("%016x/%016x", p.TXID, p.PostApplyChecksum)
}

// IsZero returns true if the position is empty.
func (p Pos) IsZero() bool {
	return p == (Pos{})
}

// Client represents a client for connecting to other LiteFS nodes.
type Client interface {
	// Stream starts a long-running connection to stream changes from another node.
	Stream(ctx context.Context, rawurl string, id string, posMap map[string]Pos) (io.ReadCloser, error)
}

type StreamFrameType uint32

const (
	StreamFrameTypeLTX   = StreamFrameType(1)
	StreamFrameTypeReady = StreamFrameType(2)
	StreamFrameTypeEnd   = StreamFrameType(3)
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
	case StreamFrameTypeLTX:
		f = &LTXStreamFrame{}
	case StreamFrameTypeReady:
		f = &ReadyStreamFrame{}
	case StreamFrameTypeEnd:
		f = &EndStreamFrame{}
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

type LTXStreamFrame struct {
	Name string // database name
}

// Type returns the type of stream frame.
func (*LTXStreamFrame) Type() StreamFrameType { return StreamFrameTypeLTX }

func (f *LTXStreamFrame) ReadFrom(r io.Reader) (int64, error) {
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

func (f *LTXStreamFrame) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, uint32(len(f.Name))); err != nil {
		return 0, err
	} else if _, err := w.Write([]byte(f.Name)); err != nil {
		return 0, err
	}
	return 0, nil
}

type ReadyStreamFrame struct{}

func (f *ReadyStreamFrame) Type() StreamFrameType               { return StreamFrameTypeReady }
func (f *ReadyStreamFrame) ReadFrom(r io.Reader) (int64, error) { return 0, nil }
func (f *ReadyStreamFrame) WriteTo(w io.Writer) (int64, error)  { return 0, nil }

type EndStreamFrame struct{}

func (f *EndStreamFrame) Type() StreamFrameType               { return StreamFrameTypeReady }
func (f *EndStreamFrame) ReadFrom(r io.Reader) (int64, error) { return 0, nil }
func (f *EndStreamFrame) WriteTo(w io.Writer) (int64, error)  { return 0, nil }

// Invalidator is a callback for the store to use to invalidate the kernel page cache.
type Invalidator interface {
	InvalidateDB(db *DB) error
	InvalidateDBRange(db *DB, offset, size int64) error
	InvalidateSHM(db *DB) error
	InvalidatePos(db *DB) error
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
