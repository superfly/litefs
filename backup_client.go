package litefs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/superfly/litefs/internal"
	"github.com/superfly/ltx"
)

type BackupClient interface {
	// URL of the backup service.
	URL() string

	// PosMap returns the replication position for all databases on the backup service.
	PosMap(ctx context.Context) (map[string]ltx.Pos, error)

	// WriteTx writes an LTX file to the backup service. The file must be
	// contiguous with the latest LTX file on the backup service or else it
	// will return an ltx.PosMismatchError.
	//
	// Returns the high-water mark that indicates it is safe to remove LTX files
	// before that transaction ID.
	WriteTx(ctx context.Context, name string, r io.Reader) (hwm ltx.TXID, err error)

	// FetchSnapshot requests a full snapshot of the database as it exists on
	// the backup service. This should be used if the LiteFS node has become
	// out of sync with the backup service.
	FetchSnapshot(ctx context.Context, name string) (io.ReadCloser, error)
}

var _ BackupClient = (*FileBackupClient)(nil)

// FileBackupClient is a reference implemenation for BackupClient.
// This implementation is typically only used for testing.
type FileBackupClient struct {
	mu   sync.Mutex
	path string
}

// NewFileBackupClient returns a new instance of FileBackupClient.
func NewFileBackupClient(path string) *FileBackupClient {
	return &FileBackupClient{path: path}
}

// Open validates & creates the path the client was initialized with.
func (c *FileBackupClient) Open() (err error) {
	// Ensure root path exists and we can get the absolute path from it.
	if c.path == "" {
		return fmt.Errorf("backup path required")
	} else if c.path, err = filepath.Abs(c.path); err != nil {
		return err
	}

	if err := os.MkdirAll(c.path, 0o777); err != nil {
		return err
	}
	return nil
}

// URL of the backup service.
func (c *FileBackupClient) URL() string {
	return (&url.URL{
		Scheme: "file",
		Path:   filepath.ToSlash(c.path),
	}).String()
}

// PosMap returns the replication position for all databases on the backup service.
func (c *FileBackupClient) PosMap(ctx context.Context) (map[string]ltx.Pos, error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	ents, err := os.ReadDir(c.path)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	m := make(map[string]ltx.Pos)
	for _, ent := range ents {
		if !ent.IsDir() {
			continue
		}
		pos, err := c.pos(ctx, ent.Name())
		if err != nil {
			return nil, err
		}
		m[ent.Name()] = pos
	}

	return m, nil
}

// pos returns the replication position for a single database.
func (c *FileBackupClient) pos(ctx context.Context, name string) (ltx.Pos, error) {
	dir := filepath.Join(c.path, name)
	ents, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return ltx.Pos{}, err
	}

	var max string
	for _, ent := range ents {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".ltx" {
			continue
		}
		if max == "" || ent.Name() > max {
			max = ent.Name()
		}
	}

	// No LTX files, return empty position.
	if max == "" {
		return ltx.Pos{}, nil
	}

	// Determine position from last file in directory.
	f, err := os.Open(filepath.Join(dir, max))
	if err != nil {
		return ltx.Pos{}, err
	}
	defer func() { _ = f.Close() }()

	dec := ltx.NewDecoder(f)
	if err := dec.Verify(); err != nil {
		return ltx.Pos{}, err
	}

	return ltx.Pos{
		TXID:              dec.Header().MaxTXID,
		PostApplyChecksum: dec.Trailer().PostApplyChecksum,
	}, nil
}

// WriteTx writes an LTX file to the backup service. The file must be
// contiguous with the latest LTX file on the backup service or else it
// will return an ltx.PosMismatchError.
func (c *FileBackupClient) WriteTx(ctx context.Context, name string, r io.Reader) (hwm ltx.TXID, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Determine the current position.
	pos, err := c.pos(ctx, name)
	if err != nil {
		return 0, err
	}

	// Peek at the LTX header.
	buf := make([]byte, ltx.HeaderSize)
	var hdr ltx.Header
	if _, err := io.ReadFull(r, buf); err != nil {
		return 0, err
	} else if err := hdr.UnmarshalBinary(buf); err != nil {
		return 0, err
	}
	r = io.MultiReader(bytes.NewReader(buf), r)

	// Ensure LTX file is contiguous with current replication position.
	if pos.TXID+1 != hdr.MinTXID || pos.PostApplyChecksum != hdr.PreApplyChecksum {
		return 0, ltx.NewPosMismatchError(pos)
	}

	// Write file to a temporary file.
	filename := filepath.Join(c.path, name, ltx.FormatFilename(hdr.MinTXID, hdr.MaxTXID))
	tempFilename := filename + ".tmp"
	if err := os.MkdirAll(filepath.Dir(tempFilename), 0o777); err != nil {
		return 0, err
	}
	f, err := os.Create(tempFilename)
	if err != nil {
		return 0, err
	}
	defer func() { _ = f.Close() }()

	// Copy & sync the file.
	if _, err := io.Copy(f, r); err != nil {
		return 0, err
	} else if err := f.Sync(); err != nil {
		return 0, err
	}

	// Verify the contents of the file.
	if _, err := f.Seek(0, io.SeekStart); err != nil {
		return 0, err
	} else if err := ltx.NewDecoder(f).Verify(); err != nil {
		return 0, err
	}
	if err := f.Close(); err != nil {
		return 0, err
	}

	// Atomically rename the file.
	if err := os.Rename(tempFilename, filename); err != nil {
		return 0, err
	} else if err := internal.Sync(filepath.Dir(filename)); err != nil {
		return 0, err
	}

	return hdr.MaxTXID, nil
}

// FetchSnapshot requests a full snapshot of the database as it exists on
// the backup service. This should be used if the LiteFS node has become
// out of sync with the backup service.
func (c *FileBackupClient) FetchSnapshot(ctx context.Context, name string) (_ io.ReadCloser, retErr error) {
	c.mu.Lock()
	defer c.mu.Unlock()

	dir := filepath.Join(c.path, name)
	ents, err := os.ReadDir(dir)
	if err != nil && !os.IsNotExist(err) {
		return nil, err
	}

	// Collect the filenames of all LTX files.
	var filenames []string
	for _, ent := range ents {
		if ent.IsDir() || filepath.Ext(ent.Name()) != ".ltx" {
			continue
		}
		filenames = append(filenames, ent.Name())
	}

	// Return an error if we have no LTX data for the database.
	if len(filenames) == 0 {
		return nil, os.ErrNotExist
	}
	sort.Strings(filenames)

	// Build sorted reader list for compactor.
	var rdrs []io.Reader
	defer func() {
		if retErr != nil {
			for _, r := range rdrs {
				_ = r.(io.Closer).Close()
			}
		}
	}()
	for _, filename := range filenames {
		f, err := os.Open(filepath.Join(dir, filename))
		if err != nil {
			return nil, err
		}
		rdrs = append(rdrs, f)
	}

	// Send compaction through a pipe so we can convert it to an io.Reader.
	pr, pw := io.Pipe()
	go func() {
		compactor := ltx.NewCompactor(pw, rdrs)
		compactor.HeaderFlags = ltx.HeaderFlagCompressLZ4
		_ = pw.CloseWithError(compactor.Compact(ctx))
	}()
	return pr, nil
}
