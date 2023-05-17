package litefs_test

import (
	"bytes"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/litefs"
)

func TestReadWriteStreamFrame(t *testing.T) {
	t.Run("LTXStreamFrame", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{Size: 100, Name: "test.db"}

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
	t.Run("ReadyStreamFrame", func(t *testing.T) {
		frame := &litefs.ReadyStreamFrame{}

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
		if err := litefs.WriteStreamFrame(&errWriter{}, &litefs.LTXStreamFrame{}); err == nil || err.Error() != `write error occurred` {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestLTXStreamFrame_ReadFrom(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{Name: "test.db"}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < buf.Len(); i++ {
			var other litefs.LTXStreamFrame
			if _, err := other.ReadFrom(bytes.NewReader(buf.Bytes()[:i])); err != io.ErrUnexpectedEOF {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

func TestLTXStreamFrame_WriteTo(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.LTXStreamFrame{Name: "test.db"}
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

func TestReadyStreamFrame_ReadFrom(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.ReadyStreamFrame{}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		for i := 1; i < buf.Len(); i++ {
			var other litefs.ReadyStreamFrame
			if _, err := other.ReadFrom(bytes.NewReader(buf.Bytes()[:i])); err != io.ErrUnexpectedEOF {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

func TestReadyStreamFrame_WriteTo(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.ReadyStreamFrame{}
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

func TestHWMStreamFrame_ReadFrom(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.HWMStreamFrame{TXID: 1234, Name: "test.db"}
		var buf bytes.Buffer
		if _, err := frame.WriteTo(&buf); err != nil {
			t.Fatal(err)
		}
		for i := 0; i < buf.Len(); i++ {
			var other litefs.HWMStreamFrame
			if _, err := other.ReadFrom(bytes.NewReader(buf.Bytes()[:i])); err != io.ErrUnexpectedEOF {
				t.Fatalf("expected error at %d bytes: %s", i, err)
			}
		}
	})
}

func TestHWMStreamFrame_WriteTo(t *testing.T) {
	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		frame := &litefs.HWMStreamFrame{TXID: 1234, Name: "test.db"}
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
