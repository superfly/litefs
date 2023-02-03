package http

import (
	"encoding/binary"
	"fmt"
	"io"
	"math"
	"regexp"
	"sort"
	"strings"

	"github.com/superfly/litefs"
)

func ReadPosMapFrom(r io.Reader) (map[string]litefs.Pos, error) {
	// Read entry count.
	var n uint32
	if err := binary.Read(r, binary.BigEndian, &n); err != nil {
		return nil, err
	}

	// Read entries and insert into map.
	m := make(map[string]litefs.Pos, n)
	for i := uint32(0); i < n; i++ {
		var nameN uint32
		if err := binary.Read(r, binary.BigEndian, &nameN); err != nil {
			return nil, err
		}
		name := make([]byte, nameN)
		if _, err := io.ReadFull(r, name); err != nil {
			return nil, err
		}

		var pos litefs.Pos
		if err := binary.Read(r, binary.BigEndian, &pos.TXID); err != nil {
			return nil, err
		} else if err := binary.Read(r, binary.BigEndian, &pos.PostApplyChecksum); err != nil {
			return nil, err
		}
		m[string(name)] = pos
	}

	return m, nil
}

func WritePosMapTo(w io.Writer, m map[string]litefs.Pos) error {
	// Sort keys for consistent output.
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)

	// Write entry count.
	if err := binary.Write(w, binary.BigEndian, uint32(len(m))); err != nil {
		return err
	}

	// Write all entries in sorted order.
	for _, name := range names {
		pos := m[name]

		// This shouldn't occur but ensure that we don't have a 4GB+ database name.
		if len(name) > math.MaxUint32 {
			return fmt.Errorf("database name too long")
		}

		if err := binary.Write(w, binary.BigEndian, uint32(len(name))); err != nil {
			return err
		} else if _, err := w.Write([]byte(name)); err != nil {
			return err
		}

		if err := binary.Write(w, binary.BigEndian, pos.TXID); err != nil {
			return err
		} else if err := binary.Write(w, binary.BigEndian, pos.PostApplyChecksum); err != nil {
			return err
		}
	}

	return nil
}

// CompileMatch returns a regular expression on a simple asterisk-only wildcard.
func CompileMatch(s string) (*regexp.Regexp, error) {
	// Convert any special characters to literal matches.
	s = regexp.QuoteMeta(s)

	// Convert escaped asterisks to wildcard matches.
	s = strings.ReplaceAll(s, `\*`, ".*")

	// Match to beginning & end of path.
	s = "^" + s + "$"

	return regexp.Compile(s)
}
