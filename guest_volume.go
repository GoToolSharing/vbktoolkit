package vbk

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/diskfs/go-diskfs/backend"
	ntfs "www.velocidex.com/golang/go-ntfs/parser"
)

func (gv *GuestVolume) FSType() string { return gv.fsType }

func (gv *GuestVolume) PathExists(p string) bool {
	switch gv.fsType {
	case "ntfs":
		_, err := gv.openMFTEntry(p)
		return err == nil
	case "ext4":
		if gv.ext4 == nil {
			return false
		}
		_, err := gv.ext4.Stat(toExtPath(p))
		return err == nil
	default:
		return false
	}
}

func (gv *GuestVolume) IsDir(p string) (bool, error) {
	switch gv.fsType {
	case "ntfs":
		entry, err := gv.openMFTEntry(p)
		if err != nil {
			return false, err
		}
		return entry.Flags().IsSet("DIRECTORY"), nil
	case "ext4":
		if gv.ext4 == nil {
			return false, fmt.Errorf("ext4 context is nil")
		}
		st, err := gv.ext4.Stat(toExtPath(p))
		if err != nil {
			return false, err
		}
		return st.IsDir(), nil
	default:
		return false, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}
}

func (gv *GuestVolume) ListDir(p string) ([]GuestEntry, error) {
	switch gv.fsType {
	case "ntfs":
		entry, err := gv.openMFTEntry(p)
		if err != nil {
			return nil, err
		}

		items := ntfs.ListDir(gv.ntfsCtx, entry)
		out := make([]GuestEntry, 0, len(items))
		base := normalizeGuestPath(p, "/")
		for _, it := range items {
			if it.Name == "" {
				continue
			}
			out = append(out, GuestEntry{
				Name:  it.Name,
				Path:  joinGuestPath(base, it.Name),
				IsDir: it.IsDir,
				Size:  toUint64NonNegative(it.Size),
			})
		}
		return sortGuestEntries(out), nil
	case "ext4":
		if gv.ext4 == nil {
			return nil, fmt.Errorf("ext4 context is nil")
		}
		dirEntries, err := gv.ext4.ReadDir(toExtPath(p))
		if err != nil {
			return nil, err
		}
		base := normalizeGuestPath(p, "/")
		out := make([]GuestEntry, 0, len(dirEntries))
		for _, de := range dirEntries {
			name := de.Name()
			if name == "" {
				continue
			}
			sz := uint64(0)
			if !de.IsDir() {
				sz = toUint64NonNegative(de.Size())
			}
			out = append(out, GuestEntry{
				Name:  name,
				Path:  joinGuestPath(base, name),
				IsDir: de.IsDir(),
				Size:  sz,
			})
		}
		return sortGuestEntries(out), nil
	default:
		return nil, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}
}

func (gv *GuestVolume) ReadFile(p string, limit int64) ([]byte, error) {
	switch gv.fsType {
	case "ntfs":
		rng, err := ntfs.GetDataForPath(gv.ntfsCtx, normalizeGuestPath(p, "/"))
		if err != nil {
			return nil, err
		}

		size := ntfs.RangeSize(rng)
		if limit >= 0 && limit < size {
			size = limit
		}
		if size < 0 {
			size = 0
		}

		buf := make([]byte, size)
		off := int64(0)
		for off < size {
			n, err := rng.ReadAt(buf[off:], off)
			off += int64(n)
			if err != nil && !errors.Is(err, io.EOF) {
				return nil, err
			}
			if n == 0 {
				break
			}
		}
		return buf[:off], nil
	case "ext4":
		if gv.ext4 == nil {
			return nil, fmt.Errorf("ext4 context is nil")
		}
		f, err := gv.ext4.OpenFile(toExtPath(p), os.O_RDONLY)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		if limit >= 0 {
			return io.ReadAll(io.LimitReader(f, limit))
		}
		return io.ReadAll(f)
	default:
		return nil, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}
}

func (gv *GuestVolume) CopyFile(p string, w io.Writer, start int64) (int64, error) {
	switch gv.fsType {
	case "ntfs":
		rng, err := ntfs.GetDataForPath(gv.ntfsCtx, normalizeGuestPath(p, "/"))
		if err != nil {
			return 0, err
		}

		size := ntfs.RangeSize(rng)
		if start < 0 {
			start = 0
		}
		if start > size {
			return 0, fmt.Errorf("start offset beyond file size")
		}

		buf := make([]byte, 1024*1024)
		off := start
		total := int64(0)
		for off < size {
			chunk := int64(len(buf))
			if rem := size - off; rem < chunk {
				chunk = rem
			}
			n, err := rng.ReadAt(buf[:chunk], off)
			if n > 0 {
				wn, werr := w.Write(buf[:n])
				total += int64(wn)
				off += int64(n)
				if werr != nil {
					return total, werr
				}
				if wn != n {
					return total, io.ErrShortWrite
				}
			}
			if err != nil && !errors.Is(err, io.EOF) {
				return total, err
			}
			if n == 0 {
				break
			}
		}

		return total, nil
	case "ext4":
		if gv.ext4 == nil {
			return 0, fmt.Errorf("ext4 context is nil")
		}
		f, err := gv.ext4.OpenFile(toExtPath(p), os.O_RDONLY)
		if err != nil {
			return 0, err
		}
		defer f.Close()

		if start < 0 {
			start = 0
		}
		if start > 0 {
			discarded, err := io.CopyN(io.Discard, f, start)
			if err != nil && !errors.Is(err, io.EOF) {
				return 0, err
			}
			if discarded < start {
				return 0, fmt.Errorf("start offset beyond file size")
			}
		}
		return io.CopyBuffer(w, f, make([]byte, 1024*1024))
	default:
		return 0, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}
}

func (gv *GuestVolume) FileSize(p string) (uint64, error) {
	switch gv.fsType {
	case "ntfs":
		rng, err := ntfs.GetDataForPath(gv.ntfsCtx, normalizeGuestPath(p, "/"))
		if err != nil {
			return 0, err
		}
		return uint64(ntfs.RangeSize(rng)), nil
	case "ext4":
		if gv.ext4 == nil {
			return 0, fmt.Errorf("ext4 context is nil")
		}
		st, err := gv.ext4.Stat(toExtPath(p))
		if err != nil {
			return 0, err
		}
		return toUint64NonNegative(st.Size()), nil
	default:
		return 0, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}
}

func (gv *GuestVolume) openMFTEntry(p string) (*ntfs.MFT_ENTRY, error) {
	if gv.fsType != "ntfs" || gv.ntfsCtx == nil {
		return nil, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}

	root, err := gv.ntfsCtx.GetMFT(5)
	if err != nil {
		return nil, err
	}

	target := normalizeGuestPath(p, "/")
	if target == "/" {
		return root, nil
	}
	return root.Open(gv.ntfsCtx, target)
}

func sortGuestEntries(entries []GuestEntry) []GuestEntry {
	sort.Slice(entries, func(i, j int) bool {
		if entries[i].IsDir != entries[j].IsDir {
			return entries[i].IsDir
		}
		return strings.ToLower(entries[i].Name) < strings.ToLower(entries[j].Name)
	})
	return entries
}

func toExtPath(p string) string {
	norm := normalizeGuestPath(p, "/")
	if norm == "/" {
		return "."
	}
	return strings.TrimPrefix(norm, "/")
}

type readerAtStorage struct {
	r    io.ReaderAt
	size int64
	pos  int64
	mu   sync.Mutex
}

func (s *readerAtStorage) Read(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	n, err := s.ReadAt(p, s.pos)
	s.pos += int64(n)
	return n, err
}

func (s *readerAtStorage) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if off >= s.size {
		return 0, io.EOF
	}
	max := int64(len(p))
	truncated := false
	if off+max > s.size {
		max = s.size - off
		truncated = true
	}
	n, err := s.r.ReadAt(p[:max], off)
	if n < int(max) && err == nil {
		err = io.EOF
	} else if truncated && err == nil {
		err = io.EOF
	}
	return n, err
}

func (s *readerAtStorage) Seek(offset int64, whence int) (int64, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := s.pos
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next += offset
	case io.SeekEnd:
		next = s.size + offset
	default:
		return 0, fmt.Errorf("invalid whence")
	}
	if next < 0 {
		return 0, fmt.Errorf("negative seek")
	}
	s.pos = next
	return s.pos, nil
}

func (s *readerAtStorage) Stat() (fs.FileInfo, error) {
	return &readerAtStorageInfo{size: s.size}, nil
}

func (s *readerAtStorage) Close() error { return nil }

func (s *readerAtStorage) Sys() (*os.File, error) {
	return nil, fmt.Errorf("os file unavailable for reader-backed storage")
}

func (s *readerAtStorage) Writable() (backend.WritableFile, error) {
	return nil, backend.ErrIncorrectOpenMode
}

func (s *readerAtStorage) Path() string { return "" }

type readerAtStorageInfo struct {
	size int64
}

func (i *readerAtStorageInfo) Name() string       { return "reader-at-storage" }
func (i *readerAtStorageInfo) Size() int64        { return i.size }
func (i *readerAtStorageInfo) Mode() fs.FileMode  { return 0 }
func (i *readerAtStorageInfo) ModTime() time.Time { return time.Time{} }
func (i *readerAtStorageInfo) IsDir() bool        { return false }
func (i *readerAtStorageInfo) Sys() any           { return nil }

func toUint64NonNegative(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
