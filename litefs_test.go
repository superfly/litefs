package litefs_test

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"reflect"
	"testing"

	"github.com/superfly/litefs"
)

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
	} else if (litefs.Pos{Chksum: 100}).IsZero() {
		t.Fatal("expected false")
	}
}

func TestFormatDBID(t *testing.T) {
	if got, want := litefs.FormatDBID(0), "00000000"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
	if got, want := litefs.FormatDBID(1000), "000003e8"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
	if got, want := litefs.FormatDBID(math.MaxUint32), "ffffffff"; got != want {
		t.Fatalf("got=%q, want %q", got, want)
	}
}

func TestParseDBID(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		if v, err := litefs.ParseDBID("00000000"); err != nil {
			t.Fatal(err)
		} else if got, want := v, uint32(0); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}

		if v, err := litefs.ParseDBID("000003e8"); err != nil {
			t.Fatal(err)
		} else if got, want := v, uint32(1000); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}

		if v, err := litefs.ParseDBID("ffffffff"); err != nil {
			t.Fatal(err)
		} else if got, want := v, uint32(math.MaxUint32); got != want {
			t.Fatalf("got=%q, want %q", got, want)
		}
	})
	t.Run("ErrTooShort", func(t *testing.T) {
		if _, err := litefs.ParseDBID("0e38"); err == nil || err.Error() != `invalid formatted database id length: "0e38"` {
			t.Fatal(err)
		}
	})
	t.Run("ErrTooLong", func(t *testing.T) {
		if _, err := litefs.ParseDBID("ffffffff0"); err == nil || err.Error() != `invalid formatted database id length: "ffffffff0"` {
			t.Fatal(err)
		}
	})
	t.Run("ErrInvalidFormat", func(t *testing.T) {
		if _, err := litefs.ParseDBID("xxxxxxxx"); err == nil || err.Error() != `invalid database id format: "xxxxxxxx"` {
			t.Fatal(err)
		}
	})
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
		frame := &litefs.LTXStreamFrame{Size: 1000}

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
	t.Run("ErrEOF", func(t *testing.T) {
		var other litefs.LTXStreamFrame
		if _, err := other.ReadFrom(bytes.NewReader(nil)); err != io.EOF {
			t.Fatalf("expected error: %s", err)
		}
	})

	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{Size: 1000}
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
		frame := &litefs.LTXStreamFrame{Size: 1000}
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
