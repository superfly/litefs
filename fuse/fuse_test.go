package fuse_test

import (
	"errors"
	"flag"
	"log"
	"os"
	"syscall"
	"testing"

	_ "github.com/mattn/go-sqlite3"
	"github.com/superfly/litefs"
	"github.com/superfly/litefs/fuse"
)

var (
	fuseDebug = flag.Bool("fuse.debug", false, "enable fuse debugging")
	tracing   = flag.Bool("tracing", false, "enable trace logging")
	long      = flag.Bool("long", false, "run long-running tests")
)

func init() {
	log.SetFlags(0)
}

func TestMain(m *testing.M) {
	flag.Parse()
	if *tracing {
		litefs.TraceLog = log.New(os.Stdout, "", litefs.TraceLogFlags)
	}
	os.Exit(m.Run())
}

func TestFileTypeFilename(t *testing.T) {
	t.Run("OK", func(t *testing.T) {
		for _, tt := range []struct {
			input  litefs.FileType
			output string
		}{
			{litefs.FileTypeDatabase, "database"},
			{litefs.FileTypeJournal, "journal"},
			{litefs.FileTypeWAL, "wal"},
			{litefs.FileTypeSHM, "shm"},
		} {
			if got, want := fuse.FileTypeFilename(tt.input), tt.output; got != want {
				t.Fatalf("got=%q, want %q", got, want)
			}
		}
	})

	t.Run("Panic", func(t *testing.T) {
		var r string
		func() {
			defer func() { r = recover().(string) }()
			fuse.FileTypeFilename(litefs.FileType(1000))
		}()

		if got, want := r, `FileTypeFilename(): invalid file type: 1000`; got != want {
			t.Fatalf("panic=%q, got %v", got, want)
		}
	})
}

func TestToErrno(t *testing.T) {
	t.Run("ENOENT", func(t *testing.T) {
		err := fuse.ToError(os.ErrNotExist).(*fuse.Error)
		if got, want := err.Error(), `file does not exist`; got != want {
			t.Fatalf("Error()=%q, want %q", got, want)
		} else if got, want := syscall.Errno(err.Errno()), syscall.ENOENT; got != want {
			t.Fatalf("Errno()=%v, want %v", got, want)
		}
	})

	t.Run("EACCES", func(t *testing.T) {
		err := fuse.ToError(litefs.ErrReadOnlyReplica).(*fuse.Error)
		if got, want := err.Error(), `read only replica`; got != want {
			t.Fatalf("Error()=%q, want %q", got, want)
		} else if got, want := syscall.Errno(err.Errno()), syscall.EACCES; got != want {
			t.Fatalf("Errno()=%v, want %v", got, want)
		}
	})

	t.Run("Passthrough", func(t *testing.T) {
		if _, ok := fuse.ToError(errors.New("marker")).(*fuse.Error); ok {
			t.Fatal("expected original error")
		}
	})
}

func TestParseFilename(t *testing.T) {
	for _, tt := range []struct {
		input    string
		dbName   string
		fileType litefs.FileType
	}{
		{"db", "db", litefs.FileTypeDatabase},
		{"db-journal", "db", litefs.FileTypeJournal},
		{"db-wal", "db", litefs.FileTypeWAL},
		{"db-shm", "db", litefs.FileTypeSHM},
	} {
		dbName, fileType := fuse.ParseFilename(tt.input)
		if got, want := dbName, tt.dbName; got != want {
			t.Fatalf("dbName=%q, want %q", got, want)
		}
		if got, want := fileType, tt.fileType; got != want {
			t.Fatalf("fileType=%v, want %v", got, want)
		}
	}
}
