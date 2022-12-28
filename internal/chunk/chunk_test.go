package chunk_test

import (
	"bytes"
	"io"
	"math/rand"
	"testing"

	"github.com/superfly/litefs/internal/chunk"
)

func TestCopy(t *testing.T) {
	rand := rand.New(rand.NewSource(0))

	var input, buf bytes.Buffer
	w := chunk.NewWriter(&buf)
	for i := 0; i < 1000; i++ {
		data := make([]byte, rand.Intn(10000))
		_, _ = rand.Read(data)

		// Save data to a simple buffer.
		_, _ = input.Write(data)

		// Write to a chunked buffer.
		if n, err := w.Write(data); err != nil {
			t.Fatal()
		} else if got, want := n, len(data); got != want {
			t.Fatalf("len=%d, want %d", got, want)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatal(err)
	}

	// Read bytes from the chunked buffer.
	output, err := io.ReadAll(chunk.NewReader(&buf))
	if err != nil {
		t.Fatal(err)
	} else if got, want := len(output), input.Len(); got != want {
		t.Fatalf("len(output)=%d, want %d", got, want)
	} else if got, want := output, input.Bytes(); !bytes.Equal(got, want) {
		t.Fatalf("output does not match input")
	}

	t.Logf("total bytes: %d", len(output))
}
