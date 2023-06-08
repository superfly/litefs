package internal_test

import (
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/superfly/litefs/internal"
)

func TestReadFullAt(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		buf := make([]byte, 2)
		if n, err := internal.ReadFullAt(strings.NewReader("abcde"), buf, 2); err != nil {
			t.Fatal(err)
		} else if got, want := n, 2; got != want {
			t.Fatalf("n=%v, want %v", got, want)
		} else if got, want := string(buf), "cd"; got != want {
			t.Fatalf("buf=%q, want %q", got, want)
		}
	})

	t.Run("ErrUnexpectedEOF", func(t *testing.T) {
		buf := make([]byte, 4)
		if n, err := internal.ReadFullAt(strings.NewReader("abcde"), buf, 2); err != io.ErrUnexpectedEOF {
			t.Fatalf("unexpected error: %#v", err)
		} else if got, want := n, 3; got != want {
			t.Fatalf("n=%v, want %v", got, want)
		} else if got, want := string(buf), "cde\x00"; got != want {
			t.Fatalf("buf=%q, want %q", got, want)
		}
	})

	t.Run("EOF", func(t *testing.T) {
		buf := make([]byte, 2)
		if _, err := internal.ReadFullAt(strings.NewReader(""), buf, 2); err != io.EOF {
			t.Fatalf("unexpected error: %#v", err)
		}
	})
}

func TestClose(t *testing.T) {
	t.Run("Nil", func(t *testing.T) {
		if err := internal.Close(nil); err != nil {
			t.Fatal("expected nil error")
		}
	})
	t.Run("NilError", func(t *testing.T) {
		if err := internal.Close(&errCloser{}); err != nil {
			t.Fatal("expected nil error")
		}
	})
	t.Run("Passthrough", func(t *testing.T) {
		errMarker := errors.New("marker")
		if err := internal.Close(&errCloser{err: errMarker}); err != errMarker {
			t.Fatalf("unexpected error: %s", err)
		}
	})
	t.Run("Ignore", func(t *testing.T) {
		if err := internal.Close(&errCloser{err: errors.New("accept tcp [::]:45859: use of closed network connection")}); err != nil {
			t.Fatalf("unexpected error: %s", err)
		}
	})
}

type errCloser struct {
	err error
}

func (c *errCloser) Close() error { return c.err }
