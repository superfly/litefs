package http

import (
	"encoding/binary"
	"io"
	"sort"

	"github.com/superfly/litefs"
)

func ReadPosMapFrom(r io.Reader) (map[uint32]litefs.Pos, error) {
	// Read entry count.
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}

	// Read entries and insert into map.
	m := make(map[uint32]litefs.Pos, n)
	for i := uint32(0); i < n; i++ {
		var dbID uint32
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

func WritePosMapTo(w io.Writer, m map[uint32]litefs.Pos) error {
	// Sort keys for consistent output.
	dbIDs := make([]uint32, 0, len(m))
	for dbID := range m {
		dbIDs = append(dbIDs, dbID)
	}
	sort.Slice(dbIDs, func(i, j int) bool { return dbIDs[i] < dbIDs[j] })

	// Write entry count.
	if err := binary.Write(w, binary.BigEndian, uint32(len(m))); err != nil {
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
