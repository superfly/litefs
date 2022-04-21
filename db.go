package litefs

import (
	"context"
	"database/sql"
	"encoding/binary"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/liteserver/liteserver"
	_ "github.com/mattn/go-sqlite3"
)

// DB represents a SQLite database.
type DB struct {
	root *RootNode
	name string
}

// NewDB returns a new instance of DB.
func NewDB(root *RootNode, name string) *DB {
	return &DB{
		root: root,
		name: name,
	}
}

func (db *DB) TIDPath() string {
	return db.path() + "-tid"
}

func (db *DB) TID() (uint64, error) {
	buf, err := os.ReadFile(db.TIDPath())
	if os.IsNotExist(err) {
		return 0, nil
	} else if err != nil {
		return 0, err
	}
	return strconv.ParseUint(string(buf), 16, 64)
}

func (db *DB) SetTID(tid uint64) error {
	// TODO: Atomic write
	return os.WriteFile(db.TIDPath(), []byte(fmt.Sprintf("%016x", tid)), 0644)
}

func (db *DB) path() string {
	return filepath.Join(db.root.path, db.name)
}

// monitor continuously streams changes from liteserver.
func (db *DB) monitor() {
	for {
		if err := db.stream(context.Background()); err != nil {
			log.Printf("cannot stream from liteserver, retrying: %s", err)
			time.Sleep(1 * time.Second)
		}
	}
}

func (db *DB) stream(ctx context.Context) error {
	tid, err := db.TID()
	if err != nil {
		return err
	}

	log.Printf("begin stream: tid=%d url=%s", tid, db.root.URL)

	client := liteserver.NewClient(db.root.URL)
	stream, err := client.Stream(tid)
	if err != nil {
		return fmt.Errorf("connecting stream: %w", err)
	}

	// Continuously read transactions and write to WAL file.
	for {
		if err := db.streamTx(ctx, stream); err != nil {
			return err
		}
	}
}

func (db *DB) streamTx(ctx context.Context, stream *liteserver.Stream) error {
	hdr, err := stream.Next()
	if err != nil {
		return fmt.Errorf("next: %w", err)
	}

	// Ignore tx if DB has already seen it.
	if tid, err := db.TID(); err != nil {
		return fmt.Errorf("db tid: %w", err)
	} else if hdr.TID <= tid {
		log.Printf("tx %d already processed, skipping", tid)
		if _, err := io.CopyN(io.Discard, stream.Reader(), hdr.Size); err != nil {
			return fmt.Errorf("discard tx: %w", err)
		}
		return nil
	}
	log.Printf("begin streaming tx %d: sz=%d", hdr.TID, hdr.Size)

	// Read entire tx file.
	buf := make([]byte, hdr.Size)
	if _, err := io.ReadFull(stream.Reader(), buf); err != nil {
		return fmt.Errorf("read page: %w", err)
	}
	frameN := int(hdr.Size / (liteserver.PageSize + liteserver.PageHeaderSize))

	f, err := os.OpenFile(db.path(), os.O_RDWR, 0666)
	if err != nil {
		return fmt.Errorf("open db file: %w", err)
	}
	defer f.Close()

	fhdrs := buf[frameN*PageSize:]
	// commit := binary.BigEndian.Uint32(buf[len(buf)-4:])

	for i := 0; i < frameN; i++ {
		data := buf[i*PageSize:][:PageSize]
		pgno := binary.BigEndian.Uint32(fhdrs[i*liteserver.PageHeaderSize:])

		log.Printf("write db page: pgno=%d len=%d", pgno, len(data))

		if _, err := f.Seek(int64((pgno-1)*PageSize), io.SeekStart); err != nil {
			return fmt.Errorf("seek: %w", err)
		} else if _, err := f.Write(data); err != nil {
			return fmt.Errorf("write db page: %w", err)
		}
	}

	if err := invalidateSHMFile(db.path()); err != nil {
		return fmt.Errorf("invalidate shm file: %w", err)
	}

	if err := db.SetTID(hdr.TID); err != nil {
		return fmt.Errorf("set tid: %w", err)
	}

	log.Printf("applied tx %d", hdr.TID)

	return nil
}

// invalidateSHMFile clears the iVersion field of the -shm file in order that
// the next transaction will rebuild it.
func invalidateSHMFile(dbPath string) error {
	db, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		return fmt.Errorf("reopen db: %w", err)
	}
	defer func() { _ = db.Close() }()

	if _, err := db.Exec(`PRAGMA wal_checkpoint(PASSIVE)`); err != nil {
		return fmt.Errorf("passive checkpoint: %w", err)
	}

	// HACK: We need to invalidate the shm via the mounted point. Maybe we can use FUSE to notify the change instead?
	mountDBPath := strings.ReplaceAll(dbPath, ".", "")
	log.Printf("invalidating shm: %s", mountDBPath)

	f, err := os.OpenFile(mountDBPath+"-shm", os.O_RDWR, 0666)
	if os.IsNotExist(err) {
		log.Printf("no shm, skipping invalidation")
		return nil
	} else if err != nil {
		return fmt.Errorf("open shm index: %w", err)
	}
	defer f.Close()

	buf := make([]byte, WALIndexHeaderSize)
	if _, err := io.ReadFull(f, buf); err != nil {
		return fmt.Errorf("read shm index: %w", err)
	}

	// Invalidate "isInit" fields.
	buf[12], buf[60] = 0, 0

	// Rewrite header.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("seek shm index: %w", err)
	} else if _, err := f.Write(buf); err != nil {
		return fmt.Errorf("overwrite shm index: %w", err)
	} else if err := f.Close(); err != nil {
		return fmt.Errorf("close shm index: %w", err)
	}

	// Truncate WAL file again.
	var row [3]int
	if err := db.QueryRow(`PRAGMA wal_checkpoint(TRUNCATE)`).Scan(&row[0], &row[1], &row[2]); err != nil {
		return fmt.Errorf("truncate: %w", err)
	}

	return nil
}

// TrimName removes "-shm" or "-wal" from the given name.
func TrimName(name string) string {
	name = strings.TrimSuffix(name, "-shm")
	return strings.TrimSuffix(name, "-wal")
}
