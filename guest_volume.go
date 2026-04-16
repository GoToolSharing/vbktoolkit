package vbk

import (
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"

	ext4 "github.com/Velocidex/go-ext4/parser"
	ntfs "www.velocidex.com/golang/go-ntfs/parser"
)

func (gv *GuestVolume) FSType() string { return gv.fsType }

func (gv *GuestVolume) PathExists(p string) bool {
	switch gv.fsType {
	case "ntfs":
		_, err := gv.openMFTEntry(p)
		return err == nil
	case "ext":
		_, err := gv.openExtInode(p)
		return err == nil
	case "xfs":
		if gv.xfsFS == nil {
			return false
		}
		_, err := gv.xfsFS.Stat(toXFSPath(p))
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
	case "ext":
		inode, err := gv.openExtInode(p)
		if err != nil {
			return false, err
		}
		return inode.Stat().Mode().IsDir(), nil
	case "xfs":
		if gv.xfsFS == nil {
			return false, fmt.Errorf("xfs context is nil")
		}
		st, err := gv.xfsFS.Stat(toXFSPath(p))
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
	case "ext":
		inode, err := gv.openExtInode(p)
		if err != nil {
			return nil, err
		}
		entries, err := inode.Dir(gv.ext4Ctx)
		if err != nil {
			return nil, err
		}

		base := normalizeGuestPath(p, "/")
		out := make([]GuestEntry, 0, len(entries))
		for _, fi := range entries {
			name := fi.Name()
			if name == "" || name == "." || name == ".." {
				continue
			}
			out = append(out, GuestEntry{
				Name:  name,
				Path:  joinGuestPath(base, name),
				IsDir: fi.Mode().IsDir(),
				Size:  toUint64NonNegative(fi.Size()),
			})
		}
		return sortGuestEntries(out), nil
	case "xfs":
		if gv.xfsFS == nil {
			return nil, fmt.Errorf("xfs context is nil")
		}
		dirEntries, err := gv.xfsFS.ReadDir(toXFSPath(p))
		if err != nil {
			return nil, err
		}
		base := normalizeGuestPath(p, "/")
		out := make([]GuestEntry, 0, len(dirEntries))
		for _, de := range dirEntries {
			name := de.Name()
			if name == "" || name == "." || name == ".." {
				continue
			}
			sz := uint64(0)
			if !de.IsDir() {
				if info, infoErr := de.Info(); infoErr == nil {
					sz = toUint64NonNegative(info.Size())
				}
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
	case "ext":
		inode, err := gv.openExtInode(p)
		if err != nil {
			return nil, err
		}
		reader, err := inode.GetReader(gv.ext4Ctx)
		if err != nil {
			return nil, err
		}
		size := inode.DataSize()
		if limit >= 0 && limit < size {
			size = limit
		}
		if size < 0 {
			size = 0
		}
		return readAllFromReaderAt(reader, size)
	case "xfs":
		if gv.xfsFS == nil {
			return nil, fmt.Errorf("xfs context is nil")
		}
		f, err := gv.xfsFS.Open(toXFSPath(p))
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
	case "ext":
		inode, err := gv.openExtInode(p)
		if err != nil {
			return 0, err
		}
		reader, err := inode.GetReader(gv.ext4Ctx)
		if err != nil {
			return 0, err
		}
		size := inode.DataSize()
		if start < 0 {
			start = 0
		}
		if start > size {
			return 0, fmt.Errorf("start offset beyond file size")
		}
		return copyFromReaderAt(reader, w, start, size-start)
	case "xfs":
		if gv.xfsFS == nil {
			return 0, fmt.Errorf("xfs context is nil")
		}
		f, err := gv.xfsFS.Open(toXFSPath(p))
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
	case "ext":
		inode, err := gv.openExtInode(p)
		if err != nil {
			return 0, err
		}
		return uint64(inode.DataSize()), nil
	case "xfs":
		if gv.xfsFS == nil {
			return 0, fmt.Errorf("xfs context is nil")
		}
		st, err := gv.xfsFS.Stat(toXFSPath(p))
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

func (gv *GuestVolume) openExtInode(p string) (*ext4.Inode, error) {
	if gv.fsType != "ext" || gv.ext4Ctx == nil {
		return nil, fmt.Errorf("unsupported filesystem type: %s", gv.fsType)
	}
	comps := extPathComponents(p)
	return gv.ext4Ctx.OpenInodeWithPath(comps)
}

func extPathComponents(p string) []string {
	norm := normalizeGuestPath(p, "/")
	norm = strings.TrimPrefix(norm, "/")
	if norm == "" {
		return nil
	}
	parts := strings.Split(norm, "/")
	out := make([]string, 0, len(parts))
	for _, part := range parts {
		part = strings.TrimSpace(part)
		if part == "" || part == "." {
			continue
		}
		out = append(out, part)
	}
	return out
}

func toXFSPath(p string) string {
	norm := normalizeGuestPath(p, "/")
	norm = strings.TrimPrefix(norm, "/")
	if norm == "" {
		return "."
	}
	return norm
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

func readAllFromReaderAt(r io.ReaderAt, size int64) ([]byte, error) {
	buf := make([]byte, size)
	off := int64(0)
	for off < size {
		n, err := r.ReadAt(buf[off:], off)
		off += int64(n)
		if err != nil && !errors.Is(err, io.EOF) {
			return nil, err
		}
		if n == 0 {
			break
		}
	}
	return buf[:off], nil
}

func copyFromReaderAt(r io.ReaderAt, w io.Writer, start, length int64) (int64, error) {
	if start < 0 {
		start = 0
	}
	if length < 0 {
		length = 0
	}
	buf := make([]byte, 1024*1024)
	off := start
	remaining := length
	total := int64(0)
	for remaining > 0 {
		chunk := int64(len(buf))
		if remaining < chunk {
			chunk = remaining
		}
		n, err := r.ReadAt(buf[:chunk], off)
		if n > 0 {
			wn, werr := w.Write(buf[:n])
			total += int64(wn)
			off += int64(n)
			remaining -= int64(n)
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
}

func toUint64NonNegative(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
