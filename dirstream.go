package litefs

import (
	"sync"
	"syscall"
	"unsafe"

	"github.com/hanwen/go-fuse/v2/fs"
	"github.com/hanwen/go-fuse/v2/fuse"
)

type DirStream struct {
	buf  []byte
	todo []byte

	mu sync.Mutex
	fd int
}

func NewDirStream(name string) (fs.DirStream, syscall.Errno) {
	fd, err := syscall.Open(name, syscall.O_DIRECTORY, 0755)
	if err != nil {
		return nil, fs.ToErrno(err)
	}

	s := &DirStream{
		buf: make([]byte, 4096),
		fd:  fd,
	}

	if err := s.load(); err != 0 {
		s.Close()
		return nil, err
	}
	return s, fs.OK
}

func (s *DirStream) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.fd != -1 {
		syscall.Close(s.fd)
		s.fd = -1
	}
}

func (s *DirStream) HasNext() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.todo) > 0
}

func (s *DirStream) Next() (fuse.DirEntry, syscall.Errno) {
	s.mu.Lock()
	defer s.mu.Unlock()

	// We can't use syscall.Dirent here, because it declares a
	// [256]byte name, which may run beyond the end of s.todo.
	// when that happens in the race detector, it causes a panic
	// "converted pointer straddles multiple allocations"
	de := (*dirent)(unsafe.Pointer(&s.todo[0]))

	nameBytes := s.todo[unsafe.Offsetof(dirent{}.Name):de.Reclen]
	s.todo = s.todo[de.Reclen:]

	// After the loop, l contains the index of the first '\0'.
	l := 0
	for l = range nameBytes {
		if nameBytes[l] == 0 {
			break
		}
	}
	nameBytes = nameBytes[:l]
	result := fuse.DirEntry{
		Ino:  de.Ino,
		Mode: (uint32(de.Type) << 12),
		Name: string(nameBytes),
	}
	return result, s.load()
}

func (s *DirStream) load() syscall.Errno {
	if len(s.todo) > 0 {
		return fs.OK
	}

	n, err := syscall.Getdents(s.fd, s.buf)
	if err != nil {
		return fs.ToErrno(err)
	}
	s.todo = s.buf[:n]
	return fs.OK
}

type dirent struct {
	Ino    uint64
	Off    int64
	Reclen uint16
	Type   uint8
	Name   [1]uint8
}
