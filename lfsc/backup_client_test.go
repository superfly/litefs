package lfsc_test

import (
	"bytes"
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/rand"
	"net/url"
	"reflect"
	"testing"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/lfsc"
	"github.com/superfly/ltx"
)

var integration = flag.Bool("integration", false, "run integration tests")

func TestBackupClient_URL(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := lfsc.NewBackupClient(litefs.NewStore(t.TempDir(), true), url.URL{Scheme: "http", Host: "localhost:1234"})
		if got, want := c.URL(), `http://localhost:1234`; got != want {
			t.Fatalf("URL=%s, want %s", got, want)
		}
	})
	t.Run("Strip", func(t *testing.T) {
		c := lfsc.NewBackupClient(litefs.NewStore(t.TempDir(), true), url.URL{
			Scheme: "http",
			Host:   "localhost:1234",
			Path:   "/foo/bar",
		})
		if got, want := c.URL(), `http://localhost:1234`; got != want {
			t.Fatalf("URL=%s, want %s", got, want)
		}
	})
}

func TestBackupClient_WriteTx(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := newOpenBackupClient(t)

		// Write several transaction files to the client.
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1000},
		})); err != nil {
			t.Fatal(err)
		}

		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 1000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		})); err != nil {
			t.Fatal(err)
		}

		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 3, MaxTXID: 4, PreApplyChecksum: ltx.ChecksumFlag | 2000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{3}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 3000},
		})); err != nil {
			t.Fatal(err)
		}

		// Write to a different database.
		if _, err := c.WriteTx(context.Background(), "db2", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{5}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 5000},
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
			Header: ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 1, MaxTXID: 4},
			Pages: []ltx.PageSpec{
				{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{3}, 512)},
				{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)},
			},
			Trailer: ltx.Trailer{
				PostApplyChecksum: ltx.ChecksumFlag | 3000,
				FileChecksum:      0xc8d8c55bde12fe8d,
			},
		}); !reflect.DeepEqual(got, want) {
			t.Fatalf("spec mismatch:\ngot:  %#v\nwant: %#v", got, want)
		}
	})

	t.Run("ErrPosMismatch/TXID", func(t *testing.T) {
		c := newOpenBackupClient(t)

		// Write the initial transaction.
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1000},
		})); err != nil {
			t.Fatal(err)
		}

		// Write a transaction that doesn't line up with the TXID.
		var pmErr *ltx.PosMismatchError
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 3, MaxTXID: 3, PreApplyChecksum: ltx.ChecksumFlag | 1000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		})); !errors.As(err, &pmErr) {
			t.Fatalf("unexpected error: %s", err)
		} else if got, want := pmErr.Pos, (ltx.Pos{TXID: 1, PostApplyChecksum: 0x80000000000003e8}); !reflect.DeepEqual(got, want) {
			t.Fatalf("pos=%s, want %s", got, want)
		}
	})

	t.Run("ErrPosMismatch/PostApplyChecksum", func(t *testing.T) {
		c := newOpenBackupClient(t)

		// Write the initial transaction.
		if _, err := c.WriteTx(context.Background(), "db", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1000},
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
		} else if got, want := pmErr.Pos, (ltx.Pos{TXID: 1, PostApplyChecksum: 0x80000000000003e8}); !reflect.DeepEqual(got, want) {
			t.Fatalf("pos=%s, want %s", got, want)
		}
	})

	t.Run("ErrPosMismatch/FirstTx", func(t *testing.T) {
		c := newOpenBackupClient(t)

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

func TestBackupClient_PosMap(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		c := newOpenBackupClient(t)

		if _, err := c.WriteTx(context.Background(), "db1", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{1}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 1000},
		})); err != nil {
			t.Fatal(err)
		}

		if _, err := c.WriteTx(context.Background(), "db1", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 2, MinTXID: 2, MaxTXID: 2, PreApplyChecksum: ltx.ChecksumFlag | 1000},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 2}, Data: bytes.Repeat([]byte{2}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 2000},
		})); err != nil {
			t.Fatal(err)
		}

		// Write to a different database.
		if _, err := c.WriteTx(context.Background(), "db2", ltxFileSpecReader(t, &ltx.FileSpec{
			Header:  ltx.Header{Version: 1, PageSize: 512, Commit: 1, MinTXID: 1, MaxTXID: 1},
			Pages:   []ltx.PageSpec{{Header: ltx.PageHeader{Pgno: 1}, Data: bytes.Repeat([]byte{5}, 512)}},
			Trailer: ltx.Trailer{PostApplyChecksum: ltx.ChecksumFlag | 5000},
		})); err != nil {
			t.Fatal(err)
		}

		// Read snapshot from backup service.
		if m, err := c.PosMap(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := m, map[string]ltx.Pos{
			"db1": {TXID: 0x2, PostApplyChecksum: 0x80000000000007d0},
			"db2": {TXID: 0x1, PostApplyChecksum: 0x8000000000001388},
		}; !reflect.DeepEqual(got, want) {
			t.Fatalf("map=%#v, want %#v", got, want)
		}
	})

	t.Run("NoDatabases", func(t *testing.T) {
		c := newOpenBackupClient(t)
		if m, err := c.PosMap(context.Background()); err != nil {
			t.Fatal(err)
		} else if got, want := m, map[string]ltx.Pos{}; !reflect.DeepEqual(got, want) {
			t.Fatalf("map=%#v, want %#v", got, want)
		}
	})
}

func newOpenBackupClient(tb testing.TB) *lfsc.BackupClient {
	tb.Helper()

	if !*integration {
		tb.Skip("integration tests not enabled, skipping")
	}

	c := lfsc.NewBackupClient(
		litefs.NewStore(tb.TempDir(), true),
		url.URL{Scheme: "http", Host: "localhost:21212"},
	)
	c.Cluster = fmt.Sprintf("test%d", rand.Intn(1000000))
	if err := c.Open(); err != nil {
		tb.Fatal(err)
	}

	tb.Logf("initializing client for test cluster: %q", c.Cluster)
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
