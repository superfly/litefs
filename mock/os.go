package mock

import (
	"os"

	"github.com/superfly/litefs"
	"github.com/superfly/litefs/internal"
)

var _ litefs.OS = (*OS)(nil)

type OS struct {
	Underlying litefs.OS

	CreateFunc    func(name string) (*os.File, error)
	MkdirFunc     func(path string, perm os.FileMode) error
	MkdirAllFunc  func(path string, perm os.FileMode) error
	OpenFunc      func(name string) (*os.File, error)
	OpenFileFunc  func(name string, flag int, perm os.FileMode) (*os.File, error)
	ReadDirFunc   func(name string) ([]os.DirEntry, error)
	ReadFileFunc  func(name string) ([]byte, error)
	RemoveFunc    func(name string) error
	RemoveAllFunc func(name string) error
	RenameFunc    func(oldpath, newpath string) error
	StatFunc      func(name string) (os.FileInfo, error)
	TruncateFunc  func(name string, size int64) error
	WriteFileFunc func(name string, data []byte, perm os.FileMode) error
}

// NewOS returns a mock OS that defaults to using an underlying system OS.
func NewOS() *OS {
	return &OS{
		Underlying: &internal.SystemOS{},
	}
}

func (m *OS) Create(name string) (*os.File, error) {
	if m.CreateFunc == nil {
		return m.Underlying.Create(name)
	}
	return m.CreateFunc(name)
}

func (m *OS) Mkdir(path string, perm os.FileMode) error {
	if m.MkdirFunc == nil {
		return m.Underlying.Mkdir(path, perm)
	}
	return m.MkdirFunc(path, perm)
}

func (m *OS) MkdirAll(path string, perm os.FileMode) error {
	if m.MkdirAllFunc == nil {
		return m.Underlying.MkdirAll(path, perm)
	}
	return m.MkdirAllFunc(path, perm)
}

func (m *OS) Open(name string) (*os.File, error) {
	if m.OpenFunc == nil {
		return m.Underlying.Open(name)
	}
	return m.OpenFunc(name)
}

func (m *OS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	if m.OpenFileFunc == nil {
		return m.Underlying.OpenFile(name, flag, perm)
	}
	return m.OpenFileFunc(name, flag, perm)
}

func (m *OS) ReadDir(name string) ([]os.DirEntry, error) {
	if m.ReadDirFunc == nil {
		return m.Underlying.ReadDir(name)
	}
	return m.ReadDirFunc(name)
}

func (m *OS) ReadFile(name string) ([]byte, error) {
	if m.ReadFileFunc == nil {
		return m.Underlying.ReadFile(name)
	}
	return m.ReadFileFunc(name)
}

func (m *OS) Remove(name string) error {
	if m.RemoveFunc == nil {
		return m.Underlying.Remove(name)
	}
	return m.RemoveFunc(name)
}

func (m *OS) RemoveAll(name string) error {
	if m.RemoveAllFunc == nil {
		return m.Underlying.RemoveAll(name)
	}
	return m.RemoveAllFunc(name)
}

func (m *OS) Rename(oldpath, newpath string) error {
	if m.RenameFunc == nil {
		return m.Underlying.Rename(oldpath, newpath)
	}
	return m.RenameFunc(oldpath, newpath)
}

func (m *OS) Stat(name string) (os.FileInfo, error) {
	if m.StatFunc == nil {
		return m.Underlying.Stat(name)
	}
	return m.StatFunc(name)
}

func (m *OS) Truncate(name string, size int64) error {
	if m.TruncateFunc == nil {
		return m.Underlying.Truncate(name, size)
	}
	return m.TruncateFunc(name, size)
}

func (m *OS) WriteFile(name string, data []byte, perm os.FileMode) error {
	if m.WriteFileFunc == nil {
		return m.Underlying.WriteFile(name, data, perm)
	}
	return m.WriteFileFunc(name, data, perm)
}
