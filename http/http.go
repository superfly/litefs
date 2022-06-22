package http

import (
	"encoding/binary"
	"fmt"
	"io"
	"sort"

	"github.com/superfly/litefs"
)

type StreamFrameType uint32

const (
	StreamFrameTypeLTX = StreamFrameType(1)
)

type StreamFrame interface {
	streamFrame()
}

func (*LTXStreamFrame) streamFrame() {}

// ReadStreamFrame reads a the stream type & frame from the reader.
func ReadStreamFrame(r io.Reader) (StreamFrame, error) {
	var typ StreamFrameType
	if err := binary.Read(r, binary.BigEndian, &typ); err != nil {
		return nil, err
	}

	switch typ {
	case StreamFrameTypeLTX:
		var f LTXStreamFrame
		_, err := f.ReadFrom(r)
		return &f, err
	default:
		return nil, fmt.Errorf("invalid stream frame type: 0x%02x", typ)
	}
}

type LTXStreamFrame struct {
	Size int64
}

func (f *LTXStreamFrame) ReadFrom(r io.Reader) (int, error) {
	if err := binary.Read(r, binary.BigEndian, &f.Size); err != nil {
		return 0, err
	}
	return 0, nil
}

func (f *LTXStreamFrame) WriteTo(w io.Writer) (int, error) {
	if err := binary.Write(w, binary.BigEndian, f.Size); err != nil {
		return 0, err
	}
	return 0, nil
}

func ReadPosMapFrom(r io.Reader) (map[uint64]litefs.Pos, error) {
	// Read entry count.
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}

	// Read entries and insert into map.
	m := make(map[uint64]litefs.Pos, n)
	for i := uint32(0); i < n; i++ {
		var dbID uint64
		var pos litefs.Pos
		if err := binary.Read(r, binary.BigEndian, &dbID); err != nil {
			return nil, err
		} else if err := binary.Read(r, binary.BigEndian, &pos.TXID); err != nil {
			return nil, err
		}
		m[dbID] = pos
	}

	return m, nil
}

func WritePosMapTo(w io.Writer, m map[uint64]litefs.Pos) error {
	// Sort keys for consistent output.
	dbIDs := make([]uint64, 0, len(m))
	for dbID := range m {
		dbIDs = append(dbIDs, dbID)
	}
	sort.Slice(dbIDs, func(i, j int) bool { return dbIDs[i] < dbIDs[j] })

	// Write entry count.
	if err := binary.Write(w, binary.BigEndian, len(m)); err != nil {
		return err
	}

	// Write all entries in sorted order.
	for _, dbID := range dbIDs {
		pos := m[dbID]
		if err := binary.Write(w, binary.BigEndian, dbID); err != nil {
			return err
		} else if err := binary.Write(w, binary.BigEndian, pos.TXID); err != nil {
			return err
		}
	}

	return nil
}
