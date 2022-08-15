package litefs_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"testing"
	"time"

	"github.com/superfly/litefs"
	"github.com/superfly/ltx"
)

func TestDB_WriteSnapshotTo(t *testing.T) {
	db, dbh := newDB(t, "db")

	data, _ := testdata.ReadFile("testdata/db/write-snapshot-to/database")

	jfh, err := db.CreateJournal()
	if err != nil {
		t.Fatal(err)
	} else if err := db.WriteJournal(jfh, decodeHexString(t, "d9d505f920a163d700000000f65ddb21000000000000020000001000"), 0); err != nil {
		t.Fatal(err)
	} else if err := jfh.Close(); err != nil {
		t.Fatal(err)
	}

	if err := db.WriteDatabase(dbh, data[0:4096], 0); err != nil {
		t.Fatal(err)
	} else if err := db.WriteDatabase(dbh, data[4096:8192], 4096); err != nil {
		t.Fatal(err)
	}

	if err := db.CommitJournal(litefs.JournalModeDelete); err != nil {
		t.Fatal(err)
	}

	var buf bytes.Buffer
	if header, trailer, err := db.WriteSnapshotTo(context.Background(), &buf); err != nil {
		t.Fatal(err)
	} else if got, want := header, (ltx.Header{
		Version:   1,
		PageSize:  4096,
		Commit:    2,
		DBID:      1,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: 0xdc6acfac00,
	}); got != want {
		t.Fatalf("unexpected snapshot header: %#v", got)
	} else if got, want := trailer, (ltx.Trailer{
		PostApplyChecksum: 0xe2e79e6905b952db,
		FileChecksum:      0x8fe0303f8bc5c2e4,
	}); got != want {
		t.Fatalf("unexpected snapshot trailer: %#v", got)
	}

	// Decode LTX file.
	dec := ltx.NewDecoder(&buf)
	if err := dec.DecodeHeader(); err != nil {
		t.Fatal(err)
	} else if got, want := dec.Header(), (ltx.Header{
		Version:   1,
		PageSize:  4096,
		Commit:    2,
		DBID:      1,
		MinTXID:   1,
		MaxTXID:   1,
		Timestamp: 0xdc6acfac00,
	}); got != want {
		t.Fatalf("unexpected header: %#v", got)
	}

	var pageHeader ltx.PageHeader
	pageData := make([]byte, 4096)
	if err := dec.DecodePage(&pageHeader, pageData); err != nil {
		t.Fatal(err)
	} else if got, want := pageHeader, (ltx.PageHeader{Pgno: 1}); got != want {
		t.Fatalf("unexpected page header: %#v", got)
	} else if !bytes.Equal(pageData, data[0:4096]) {
		t.Fatalf("unexpected page data: %x", pageData)
	}

	if err := dec.DecodePage(&pageHeader, pageData); err != nil {
		t.Fatal(err)
	} else if got, want := pageHeader, (ltx.PageHeader{Pgno: 2}); got != want {
		t.Fatalf("unexpected page header: %#v", got)
	} else if !bytes.Equal(pageData, data[4096:8192]) {
		t.Fatalf("unexpected page data: %x", pageData)
	}

	if err := dec.DecodePage(&pageHeader, pageData); err != io.EOF {
		t.Fatalf("unexpected error: %v", err)
	}

	if err := dec.Close(); err != nil {
		t.Fatal(err)
	} else if got, want := dec.Trailer(), (ltx.Trailer{
		PostApplyChecksum: 0xe2e79e6905b952db,
		FileChecksum:      0x8fe0303f8bc5c2e4,
	}); got != want {
		t.Fatalf("unexpected trailer: %#v", got)
	}

	// Verify checksum matches original file.
	if chksum, err := ltx.ChecksumReader(bytes.NewReader(data), 4096); err != nil {
		t.Fatal(err)
	} else if chksum != 0xe2e79e6905b952db {
		t.Fatalf("unexpected checksum: 0x%x", chksum)
	}
}

// newDB returns a new instance of DB attached to a temporary store.
func newDB(tb testing.TB, name string) (*litefs.DB, *os.File) {
	tb.Helper()

	store := newOpenStore(tb)
	db, f, err := store.CreateDB(name)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = f.Close() })

	db.Now = func() time.Time { return time.Date(2000, 1, 1, 0, 0, 0, 0, time.UTC) }

	return db, f
}
