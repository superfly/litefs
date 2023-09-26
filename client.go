package litefs

import (
	"context"
	"encoding/binary"
	"fmt"
	"io"

	"github.com/superfly/ltx"
)

// Client represents a client for connecting to other LiteFS nodes.
type Client interface {
	// AcquireHaltLock attempts to acquire a remote halt lock on the primary node.
	AcquireHaltLock(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) (*HaltLock, error)

	// ReleaseHaltLock releases a previous held remote halt lock on the primary node.
	ReleaseHaltLock(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64) error

	// Commit sends an LTX file to the primary to be committed.
	// Must be holding the halt lock to be successful.
	Commit(ctx context.Context, primaryURL string, nodeID uint64, name string, lockID int64, r io.Reader) error

	// Stream starts a long-running connection to stream changes from another node.
	// If filter is specified, only those databases will be replicated.
	Stream(ctx context.Context, primaryURL string, nodeID uint64, posMap map[string]ltx.Pos, filter []string) (Stream, error)
}

// Stream represents a stream of frames.
type Stream interface {
	io.ReadCloser

	// ClusterID of the primary node.
	ClusterID() string
}

type StreamFrameType uint32

const (
	StreamFrameTypeLTX       = StreamFrameType(1)
	StreamFrameTypeReady     = StreamFrameType(2)
	StreamFrameTypeEnd       = StreamFrameType(3)
	StreamFrameTypeDropDB    = StreamFrameType(4)
	StreamFrameTypeHandoff   = StreamFrameType(5)
	StreamFrameTypeHWM       = StreamFrameType(6)
	StreamFrameTypeHeartbeat = StreamFrameType(7)
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
	case StreamFrameTypeDropDB:
		f = &DropDBStreamFrame{}
	case StreamFrameTypeHandoff:
		f = &HandoffStreamFrame{}
	case StreamFrameTypeHWM:
		f = &HWMStreamFrame{}
	case StreamFrameTypeHeartbeat:
		f = &HeartbeatStreamFrame{}
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
	Size int64  // payload size
	Name string // database name
}

// Type returns the type of stream frame.
func (*LTXStreamFrame) Type() StreamFrameType { return StreamFrameTypeLTX }

func (f *LTXStreamFrame) ReadFrom(r io.Reader) (int64, error) {
	var size uint64
	if err := binary.Read(r, binary.BigEndian, &size); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}
	f.Size = int64(size)

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
	if err := binary.Write(w, binary.BigEndian, uint64(f.Size)); err != nil {
		return 0, err
	}

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

func (f *EndStreamFrame) Type() StreamFrameType               { return StreamFrameTypeEnd }
func (f *EndStreamFrame) ReadFrom(r io.Reader) (int64, error) { return 0, nil }
func (f *EndStreamFrame) WriteTo(w io.Writer) (int64, error)  { return 0, nil }

// DropDBStreamFrame notifies replicas that a database has been deleted.
// DEPRECATED: LTX files with a zero "commit" field now represent deletions.
type DropDBStreamFrame struct {
	Name string // database name
}

// Type returns the type of stream frame.
func (*DropDBStreamFrame) Type() StreamFrameType { return StreamFrameTypeDropDB }

func (f *DropDBStreamFrame) ReadFrom(r io.Reader) (int64, error) {
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

func (f *DropDBStreamFrame) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, uint32(len(f.Name))); err != nil {
		return 0, err
	} else if _, err := w.Write([]byte(f.Name)); err != nil {
		return 0, err
	}
	return 0, nil
}

type HandoffStreamFrame struct {
	LeaseID string
}

// Type returns the type of stream frame.
func (*HandoffStreamFrame) Type() StreamFrameType { return StreamFrameTypeHandoff }

func (f *HandoffStreamFrame) ReadFrom(r io.Reader) (int64, error) {
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}

	leaseID := make([]byte, n)
	if _, err := io.ReadFull(r, leaseID); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}
	f.LeaseID = string(leaseID)

	return 0, nil
}

func (f *HandoffStreamFrame) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, uint32(len(f.LeaseID))); err != nil {
		return 0, err
	} else if _, err := w.Write([]byte(f.LeaseID)); err != nil {
		return 0, err
	}
	return 0, nil
}

// HWMStreamFrame propagates the high-water mark to replica nodes.
type HWMStreamFrame struct {
	TXID ltx.TXID // high-water mark TXID
	Name string   // database name
}

// Type returns the type of stream frame.
func (*HWMStreamFrame) Type() StreamFrameType { return StreamFrameTypeHWM }

func (f *HWMStreamFrame) ReadFrom(r io.Reader) (int64, error) {
	var txID uint64
	if err := binary.Read(r, binary.BigEndian, &txID); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}
	f.TXID = ltx.TXID(txID)

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

func (f *HWMStreamFrame) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, uint64(f.TXID)); err != nil {
		return 0, err
	}

	if err := binary.Write(w, binary.BigEndian, uint32(len(f.Name))); err != nil {
		return 0, err
	} else if _, err := w.Write([]byte(f.Name)); err != nil {
		return 0, err
	}
	return 0, nil
}

// HeartbeatStreamFrame informs replicas that there have been no recent transactions
type HeartbeatStreamFrame struct {
	Timestamp int64 // ms since unix epoch
}

// Type returns the type of stream frame.
func (f *HeartbeatStreamFrame) Type() StreamFrameType { return StreamFrameTypeHeartbeat }

func (f *HeartbeatStreamFrame) ReadFrom(r io.Reader) (int64, error) {
	if err := binary.Read(r, binary.BigEndian, &f.Timestamp); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}

	return 0, nil
}

func (f *HeartbeatStreamFrame) WriteTo(w io.Writer) (int64, error) {
	if err := binary.Write(w, binary.BigEndian, f.Timestamp); err != nil {
		return 0, err
	}

	return 0, nil
}
