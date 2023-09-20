package mock

import (
	"os"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal"
)

var _ litefs.OS = (*OS)(nil)

type OS struct {
	Underlying litefs.OS

	CreateFunc    func(op, name string) (*os.File, error)
	MkdirFunc     func(op, path string, perm os.FileMode) error
	MkdirAllFunc  func(op, path string, perm os.FileMode) error
	OpenFunc      func(op, name string) (*os.File, error)
	OpenFileFunc  func(op, name string, flag int, perm os.FileMode) (*os.File, error)
	ReadDirFunc   func(op, name string) ([]os.DirEntry, error)
	ReadFileFunc  func(op, name string) ([]byte, error)
	RemoveFunc    func(op, name string) error
	RemoveAllFunc func(op, name string) error
	RenameFunc    func(op, oldpath, newpath string) error
	StatFunc      func(op, name string) (os.FileInfo, error)
	TruncateFunc  func(op, name string, size int64) error
	WriteFileFunc func(op, name string, data []byte, perm os.FileMode) error
}

// NewOS returns a mock OS that defaults to using an underlying system OS.
func NewOS() *OS {
	return &OS{
		Underlying: &internal.SystemOS{},
	}
}

func (m *OS) Create(op, name string) (*os.File, error) {
	if m.CreateFunc == nil {
		return m.Underlying.Create(op, name)
	}
	return m.CreateFunc(op, name)
}

func (m *OS) Mkdir(op, path string, perm os.FileMode) error {
	if m.MkdirFunc == nil {
		return m.Underlying.Mkdir(op, path, perm)
	}
	return m.MkdirFunc(op, path, perm)
}

func (m *OS) MkdirAll(op, path string, perm os.FileMode) error {
	if m.MkdirAllFunc == nil {
		return m.Underlying.MkdirAll(op, path, perm)
	}
	return m.MkdirAllFunc(op, path, perm)
}

func (m *OS) Open(op, name string) (*os.File, error) {
	if m.OpenFunc == nil {
		return m.Underlying.Open(op, name)
	}
	return m.OpenFunc(op, name)
}

func (m *OS) OpenFile(op, name string, flag int, perm os.FileMode) (*os.File, error) {
	if m.OpenFileFunc == nil {
		return m.Underlying.OpenFile(op, name, flag, perm)
	}
	return m.OpenFileFunc(op, name, flag, perm)
}

func (m *OS) ReadDir(op, name string) ([]os.DirEntry, error) {
	if m.ReadDirFunc == nil {
		return m.Underlying.ReadDir(op, name)
	}
	return m.ReadDirFunc(op, name)
}

func (m *OS) ReadFile(op, name string) ([]byte, error) {
	if m.ReadFileFunc == nil {
		return m.Underlying.ReadFile(op, name)
	}
	return m.ReadFileFunc(op, name)
}

func (m *OS) Remove(op, name string) error {
	if m.RemoveFunc == nil {
		return m.Underlying.Remove(op, name)
	}
	return m.RemoveFunc(op, name)
}

func (m *OS) RemoveAll(op, name string) error {
	if m.RemoveAllFunc == nil {
		return m.Underlying.RemoveAll(op, name)
	}
	return m.RemoveAllFunc(op, name)
}

func (m *OS) Rename(op, oldpath, newpath string) error {
	if m.RenameFunc == nil {
		return m.Underlying.Rename(op, oldpath, newpath)
	}
	return m.RenameFunc(op, oldpath, newpath)
}

func (m *OS) Stat(op, name string) (os.FileInfo, error) {
	if m.StatFunc == nil {
		return m.Underlying.Stat(op, name)
	}
	return m.StatFunc(op, name)
}

func (m *OS) Truncate(op, name string, size int64) error {
	if m.TruncateFunc == nil {
		return m.Underlying.Truncate(op, name, size)
	}
	return m.TruncateFunc(op, name, size)
}

func (m *OS) WriteFile(op, name string, data []byte, perm os.FileMode) error {
	if m.WriteFileFunc == nil {
		return m.Underlying.WriteFile(op, name, data, perm)
	}
	return m.WriteFileFunc(op, name, data, perm)
}
