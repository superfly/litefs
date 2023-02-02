package chunk

import (
	"encoding/binary"
	"io"
	"math"
)

// EOF is the end-of-file marker value for the size.
const EOF = uint16(0x0000)

// MaxChunkSize is the largest allowable chunk size (64KB).
const MaxChunkSize = math.MaxUint16

var _ io.Reader = (*Reader)(nil)

// Reader wraps a stream of chunks and converts it into an io.Reader.
// This is useful for byte streams where the size is not known beforehand.
type Reader struct {
	r   io.Reader          // underlying reader
	b   [MaxChunkSize]byte // underlying buffer
	buf []byte             // current buffer
	eof bool               // true for last chunk
}

// NewReader implements an io.Reader from a chunked byte stream.
func NewReader(r io.Reader) *Reader {
	return &Reader{r: r}
}

func (r *Reader) Read(p []byte) (n int, err error) {
	// If we have bytes in the buffer, then copy them to p.
	if len(r.buf) > 0 {
		n = copy(p, r.buf)
		r.buf = r.buf[n:]
		return n, nil
	}

	// If no bytes are buffered and we are on the last chunk then return EOF.
	if r.eof {
		return 0, io.EOF
	}

	// Otherwise read next chunk and refill the buffer.
	var size uint16
	if err := binary.Read(r.r, binary.BigEndian, &size); err == io.EOF {
		return 0, io.ErrUnexpectedEOF
	} else if err != nil {
		return 0, err
	}

	// Exit if this is the closing EOF chunk.
	r.eof = size == EOF
	if r.eof {
		return 0, io.EOF
	}

	// The remaining bits are used for the chunk size (up to 64KB).
	r.buf = r.b[:size]
	if _, err := io.ReadFull(r.r, r.buf); err != nil {
		return 0, err
	}

	// Copy out as much to the buffer as possible.
	n = copy(p, r.buf)
	r.buf = r.buf[n:]
	return n, nil
}

var _ io.WriteCloser = (*Writer)(nil)

// Writer wraps an io.Writer to convert it to a chunked byte stream.
// This is useful for byte streams where the size is not known beforehand.
type Writer struct {
	w      io.Writer
	closed bool
}

// NewWriter implements an io.Writer from a chunked byte stream.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

// Close closes the writer and writes out a closing EOF chunk.
func (w *Writer) Close() error {
	if w.closed {
		return nil // no-op double close
	}

	err := binary.Write(w.w, binary.BigEndian, EOF)
	w.closed = true
	return err
}

// Write writes p to the underlying writer with a chunk header.
func (w *Writer) Write(p []byte) (n int, err error) {
	// Ignore empty byte slices as zero size is reserved for EOF.
	if len(p) == 0 {
		return 0, nil
	}

	for len(p) > 0 {
		// Write up to the max chunk size for any given chunk.
		chunk := p
		if len(chunk) > MaxChunkSize {
			chunk = chunk[:MaxChunkSize]
		}
		p = p[len(chunk):]

		// Write two bytes for the chunk length.
		if err := binary.Write(w.w, binary.BigEndian, uint16(len(chunk))); err != nil {
			return n, err
		}

		// Write the chunk itself.
		nn, err := w.w.Write(chunk)
		if n += nn; err != nil {
			return n, err
		}
	}

	return n, nil
}
