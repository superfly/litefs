package litefs_test

import (
	"bytes"
	"embed"
	"encoding/hex"
	"fmt"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/litefs"
)

//go:embed testdata
var testdata embed.FS

func TestFileType_IsValid(t *testing.T) {
	t.Run("Valid", func(t *testing.T) {
		for _, typ := range []litefs.FileType{
			litefs.FileTypeDatabase,
			litefs.FileTypeJournal,
			litefs.FileTypeWAL,
			litefs.FileTypeSHM,
		} {
			if !typ.IsValid() {
				t.Fatalf("expected valid for %d", typ)
			}
		}
	})
	t.Run("Invalid", func(t *testing.T) {
		if litefs.FileType(100).IsValid() {
			t.Fatalf("expected invalid")
		}
	})
	t.Run("None", func(t *testing.T) {
		if litefs.FileTypeNone.IsValid() {
			t.Fatalf("expected invalid")
		}
	})
}

func TestPos_IsZero(t *testing.T) {
	if !(litefs.Pos{}).IsZero() {
		t.Fatal("expected true")
	}
	if (litefs.Pos{TXID: 100}).IsZero() {
		t.Fatal("expected false")
	} else if (litefs.Pos{PostApplyChecksum: 100}).IsZero() {
		t.Fatal("expected false")
	}
}

func TestReadWriteStreamFrame(t *testing.T) {
	t.Run("DBStreamFrame", func(t *testing.T) {
		frame := &litefs.DBStreamFrame{DBID: 1000, Name: "test.db"}

		var buf bytes.Buffer
		if err := litefs.WriteStreamFrame(&buf, frame); err != nil {
			t.Fatal(err)
		}
		if other, err := litefs.ReadStreamFrame(&buf); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(frame, other) {
			t.Fatalf("got %#v, want %#v", frame, other)
		}
	})
	t.Run("LTXStreamFrame", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{}

		var buf bytes.Buffer
		if err := litefs.WriteStreamFrame(&buf, frame); err != nil {
			t.Fatal(err)
		}
		if other, err := litefs.ReadStreamFrame(&buf); err != nil {
			t.Fatal(err)
		} else if !reflect.DeepEqual(frame, other) {
			t.Fatalf("got %#v, want %#v", frame, other)
		}
	})

	t.Run("ErrEOF", func(t *testing.T) {
		if _, err := litefs.ReadStreamFrame(bytes.NewReader(nil)); err == nil || err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
	t.Run("ErrStreamTypeOnly", func(t *testing.T) {
		if _, err := litefs.ReadStreamFrame(bytes.NewReader([]byte{0, 0, 0, 1})); err == nil || err != io.ErrUnexpectedEOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
	t.Run("ErrInvalidStreamType", func(t *testing.T) {
		if _, err := litefs.ReadStreamFrame(bytes.NewReader([]byte{1, 2, 3, 4})); err == nil || err.Error() != `invalid stream frame type: 0x1020304` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
	t.Run("ErrPartialPayload", func(t *testing.T) {
		if _, err := litefs.ReadStreamFrame(bytes.NewReader([]byte{0, 0, 0, 1, 1, 2})); err == nil || err != io.ErrUnexpectedEOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
	t.Run("ErrWriteType", func(t *testing.T) {
		if err := litefs.WriteStreamFrame(&errWriter{}, &litefs.DBStreamFrame{}); err == nil || err.Error() != `write error occurred` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestDBStreamFrame_ReadFrom(t *testing.T) {
	t.Run("ErrEOF", func(t *testing.T) {
		var other litefs.DBStreamFrame
		if _, err := other.ReadFrom(bytes.NewReader(nil)); err != io.EOF {
			t.Fatalf("expected error: %s", err)
		}
	})

	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.DBStreamFrame{DBID: 1000, Name: "test.db"}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		for i := 1; i < buf.Len(); i++ {
			var other litefs.DBStreamFrame
			if _, err := other.ReadFrom(bytes.NewReader(buf.Bytes()[:i])); err != io.ErrUnexpectedEOF {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

func TestDBStreamFrame_WriteTo(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.DBStreamFrame{DBID: 1000, Name: "test.db"}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < buf.Len(); i++ {
			if _, err := frame.WriteTo(&errWriter{afterN: i}); err == nil || err.Error() != `write error occurred` {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

func TestLTXStreamFrame_ReadFrom(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		for i := 1; i < buf.Len(); i++ {
			var other litefs.LTXStreamFrame
			if _, err := other.ReadFrom(bytes.NewReader(buf.Bytes()[:i])); err != io.ErrUnexpectedEOF {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

func TestLTXStreamFrame_WriteTo(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}

		for i := 0; i < buf.Len(); i++ {
			if _, err := frame.WriteTo(&errWriter{afterN: i}); err == nil || err.Error() != `write error occurred` {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

type errWriter struct{ afterN int }

func (w *errWriter) Write(p []byte) (int, error) {
	if w.afterN -= len(p); w.afterN <= 0 {
		return 0, fmt.Errorf("write error occurred")
	}
	return len(p), nil
}

func decodeHexString(tb testing.TB, s string) []byte {
	tb.Helper()
	b, err := hex.DecodeString(s)
	if err != nil {
		tb.Fatal(err)
	}
	return b
}
