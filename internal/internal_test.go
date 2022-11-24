package internal_test

import (
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
