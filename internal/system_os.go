package internal

import "os"

// SystemOS represents an implementation of OS that simply calls the os package functions.
type SystemOS struct{}

func (*SystemOS) Create(name string) (*os.File, error) {
	return os.Create(name)
}

func (*SystemOS) Mkdir(path string, perm os.FileMode) error {
	return os.Mkdir(path, perm)
}

func (*SystemOS) MkdirAll(path string, perm os.FileMode) error {
	return os.MkdirAll(path, perm)
}

func (*SystemOS) Open(name string) (*os.File, error) {
	return os.Open(name)
}

func (*SystemOS) OpenFile(name string, flag int, perm os.FileMode) (*os.File, error) {
	return os.OpenFile(name, flag, perm)
}

func (*SystemOS) ReadDir(name string) ([]os.DirEntry, error) {
	return os.ReadDir(name)
}

func (*SystemOS) ReadFile(name string) ([]byte, error) {
	return os.ReadFile(name)
}

func (*SystemOS) Remove(name string) error {
	return os.Remove(name)
}

func (*SystemOS) RemoveAll(name string) error {
	return os.RemoveAll(name)
}

func (*SystemOS) Rename(oldpath, newpath string) error {
	return os.Rename(oldpath, newpath)
}

func (*SystemOS) Stat(name string) (os.FileInfo, error) {
	return os.Stat(name)
}

func (*SystemOS) Truncate(name string, size int64) error {
	return os.Truncate(name, size)
}

func (*SystemOS) WriteFile(name string, data []byte, perm os.FileMode) error {
	return os.WriteFile(name, data, perm)
}
