package litefs_test

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
)

func TestFileBackupClient_URL(t *testing.T) {
	c := litefs.NewFileBackupClient("/path/to/data")
	if got, want := c.URL(), `file:///path/to/data`; got != want {
		t.Fatalf("URL=%s, want %s", got, want)
	}
}

func TestFileBackupClient_WriteTx(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := newOpenFileBackupClient(t)

		// Write several transaction files to the client.
		if hwm, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xe4e4aaa102377eee},
		})); err != nil {
			t.Fatal(err)
		} else if got, want := hwm, ltx.TXID(1); got != want {
			t.Fatalf("hwm=%s, want %s", got, want)
		}

		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xe4e4aaa102377eee},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x99b1d11ab98cc555},
		})); err != nil {
			t.Fatal(err)
		}

		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 3, MaxTXID: 4, PreApplyChecksum: 0x99b1d11ab98cc555},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{3}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x8b87423eeeeeeeee},
		})); err != nil {
			t.Fatal(err)
		}

		// Write to a different database.
		if _, err := c.WriteTx(context.Background(), "db2", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{5}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x99b1d11ab98cc555},
		})); err != nil {
			t.Fatal(err)
		}

		// Read snapshot from backup service.
		var other ltx.FileSpec
		if rc, err := c.FetchSnapshot(context.Background(), "db"); err != nil {
			t.Fatal(err)
		} else if _, err := other.ReadFrom(rc); err != nil {
			t.Fatal(err)
		} else if err := rc.Close(); err != nil {
			t.Fatal(err)
		}

		// Verify contents of the snapshot.
		if got, want := &other, (&ltx.FileSpec{
			Header: ltx.Header{Version: 1, Flags: ltx.HeaderFlagCompressLZ4, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 4},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{3}, 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: 0x8b87423eeeeeeeee,
				FileChecksum:      0xb8e6a652b0ec8453,
			},
		}); !reflect.DeepEqual(got, want) {
			t.Fatalf("spec mismatch:\ngot:  %#v\nwant: %#v", got, want)
		}
	})

	t.Run("ErrPosMismatch/TXID", func(t *testing.T) {
		c := newOpenFileBackupClient(t)

		// Write the initial transaction.
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xe4e4aaa102377eee},
		})); err != nil {
			t.Fatal(err)
		}

		// Write a transaction that doesn't line up with the TXID.
		var pmErr *ltx.PosMismatchError
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 3, MaxTXID: 3, PreApplyChecksum: 0xe4e4aaa102377eee},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		})); !errors.As(err, &pmErr) {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := pmErr.Pos, (ltx.Pos{TXID: 1, PostApplyChecksum: 0xe4e4aaa102377eee}); !reflect.DeepEqual(got, want) {
			t.Fatalf("pos=%s, want %s", got, want)
		}
	})

	t.Run("ErrPosMismatch/PostApplyChecksum", func(t *testing.T) {
		c := newOpenFileBackupClient(t)

		// Write the initial transaction.
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xe4e4aaa102377eee},
		})); err != nil {
			t.Fatal(err)
		}

		// Write a transaction that doesn't line up with the TXID.
		var pmErr *ltx.PosMismatchError
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		})); !errors.As(err, &pmErr) {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := pmErr.Pos, (ltx.Pos{TXID: 1, PostApplyChecksum: 0xe4e4aaa102377eee}); !reflect.DeepEqual(got, want) {
			t.Fatalf("pos=%s, want %s", got, want)
		}
	})

	t.Run("ErrPosMismatch/FirstTx", func(t *testing.T) {
		c := newOpenFileBackupClient(t)

		var pmErr *ltx.PosMismatchError
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 2000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1000},
		})); !errors.As(err, &pmErr) {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := pmErr.Pos, (ltx.Pos{}); !reflect.DeepEqual(got, want) {
			t.Fatalf("pos=%s, want %s", got, want)
		}
	})
}

func TestFileBackupClient_PosMap(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := newOpenFileBackupClient(t)

		if _, err := c.WriteTx(context.Background(), "db1", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0xe4e4aaa102377eee},
		})); err != nil {
			t.Fatal(err)
		}

		if _, err := c.WriteTx(context.Background(), "db1", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: 0xe4e4aaa102377eee},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		})); err != nil {
			t.Fatal(err)
		}

		// Write to a different database.
		if _, err := c.WriteTx(context.Background(), "db2", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{5}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: 0x99b1d11ab98cc555},
		})); err != nil {
			t.Fatal(err)
		}

		// Read snapshot from backup service.
		if m, err := c.PosMap(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := m, map[string]ltx.Pos{
			"db1": {TXID: 0x2, PostApplyChecksum: 0x80000000000007d0},
			"db2": {TXID: 0x1, PostApplyChecksum: 0x99b1d11ab98cc555},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("map=%#v, want %#v", got, want)
		}
	})

	t.Run("NoDatabases", func(t *testing.T) {
		c := newOpenFileBackupClient(t)
		if m, err := c.PosMap(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := m, map[string]ltx.Pos{}; !reflect.DeepEqual(got, want) {
			t.Fatalf("map=%#v, want %#v", got, want)
		}
	})
}

func newOpenFileBackupClient(tb testing.TB) *litefs.FileBackupClient {
	tb.Helper()
	c := litefs.NewFileBackupClient(tb.TempDir())
	if err := c.Open(); err != nil {
		tb.Fatal(err)
	}
	return c
}

// ltxFileSpecReader returns a spec as an io.Reader of its serialized bytes.
func ltxFileSpecReader(tb testing.TB, spec *ltx.FileSpec) io.Reader {
	tb.Helper()
	var buf bytes.Buffer
	if _, err := spec.WriteTo(&buf); err != nil {
		tb.Fatal(err)
	}
	return bytes.NewReader(buf.Bytes())
}
