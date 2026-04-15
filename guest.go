package vbk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"sort"
	"strings"
	"sync"
	"unicode/utf16"

	vhdx "github.com/Velocidex/go-vhdx/parser"
	ntfs "www.velocidex.com/golang/go-ntfs/parser"
)

type Guest struct {
	volumes      []*GuestVolume
	defaultIndex int
	closers      []io.Closer
}

type GuestVolume struct {
	Index       int
	DiskPath    string
	VolumeIndex int
	Name        string
	Size        uint64

	fsType  string
	ntfsCtx *ntfs.NTFSContext
}

type GuestEntry struct {
	Name  string
	Path  string
	IsDir bool
	Size  uint64
}

type guestDiskFile struct {
	Path string
	Item *DirItem
}

func (v *VBK) DiscoverGuest() (*Guest, error) {
	g := &Guest{volumes: make([]*GuestVolume, 0, 4)}

	disks, err := v.findVirtualDiskItems()
	if err != nil {
		return nil, err
	}

	for _, disk := range disks {
		stream, err := disk.Item.Open()
		if err != nil {
			continue
		}
		g.closers = append(g.closers, stream)

		virtualReader, sectorSize, err := openVirtualDiskReader(&lockedReadSeekerAt{r: stream})
		if err != nil {
			continue
		}

		parts, err := parseGPTPartitions(virtualReader, sectorSize)
		if err != nil || len(parts) == 0 {
			parts, err = parseMBRPartitions(virtualReader, sectorSize)
			if err != nil {
				continue
			}
		}

		for _, p := range parts {
			vol := &GuestVolume{
				Index:       len(g.volumes),
				DiskPath:    disk.Path,
				VolumeIndex: int(p.Index),
				Name:        p.Name,
				Size:        p.Size,
				fsType:      "unknown",
			}

			offsetReader := &ntfs.OffsetReader{Offset: int64(p.Start), Reader: virtualReader}
			paged, _ := ntfs.NewPagedReader(offsetReader, 1024, 10000)
			ctx, err := ntfs.GetNTFSContext(paged, 0)
			if err == nil {
				vol.fsType = "ntfs"
				vol.ntfsCtx = ctx
			}

			if vol.Name == "" {
				if vol.fsType == "ntfs" {
					vol.Name = "Basic data partition"
				} else {
					vol.Name = "Partition"
				}
			}

			g.volumes = append(g.volumes, vol)
		}
	}

	for i, vol := range g.volumes {
		if vol.fsType != "ntfs" {
			continue
		}
		if vol.PathExists("/Windows") || vol.PathExists("/Users") {
			g.defaultIndex = i
			break
		}
	}

	return g, nil
}

func (g *Guest) Close() error {
	var firstErr error
	for _, c := range g.closers {
		if err := c.Close(); err != nil && firstErr == nil {
			firstErr = err
		}
	}
	g.closers = nil
	return firstErr
}

func (g *Guest) Volumes() []*GuestVolume {
	out := make([]*GuestVolume, len(g.volumes))
	copy(out, g.volumes)
	return out
}

func (g *Guest) DefaultIndex() int { return g.defaultIndex }

func (gv *GuestVolume) FSType() string { return gv.fsType }

func (gv *GuestVolume) PathExists(p string) bool {
	_, err := gv.openMFTEntry(p)
	return err == nil
}

func (gv *GuestVolume) IsDir(p string) (bool, error) {
	entry, err := gv.openMFTEntry(p)
	if err != nil {
		return false, err
	}
	return entry.Flags().IsSet("DIRECTORY"), nil
}

func (gv *GuestVolume) ListDir(p string) ([]GuestEntry, error) {
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

	sort.Slice(out, func(i, j int) bool {
		if out[i].IsDir != out[j].IsDir {
			return out[i].IsDir
		}
		return strings.ToLower(out[i].Name) < strings.ToLower(out[j].Name)
	})

	return out, nil
}

func (gv *GuestVolume) ReadFile(p string, limit int64) ([]byte, error) {
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
}

func (gv *GuestVolume) CopyFile(p string, w io.Writer, start int64) (int64, error) {
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
}

func (gv *GuestVolume) FileSize(p string) (uint64, error) {
	rng, err := ntfs.GetDataForPath(gv.ntfsCtx, normalizeGuestPath(p, "/"))
	if err != nil {
		return 0, err
	}
	return uint64(ntfs.RangeSize(rng)), nil
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

func (v *VBK) findVirtualDiskItems() ([]guestDiskFile, error) {
	root, err := v.Get("/", nil)
	if err != nil {
		return nil, err
	}

	out := make([]guestDiskFile, 0, 4)
	var walk func(curPath string, item *DirItem) error
	walk = func(curPath string, item *DirItem) error {
		if !item.IsDir() {
			low := strings.ToLower(item.Name)
			if strings.HasSuffix(low, ".vhd") || strings.HasSuffix(low, ".vhdx") {
				out = append(out, guestDiskFile{Path: curPath, Item: item})
			}
			return nil
		}

		entries, err := item.IterDir()
		if err != nil {
			return err
		}
		for _, child := range entries {
			next := joinGuestPath(curPath, child.Name)
			if err := walk(next, child); err != nil {
				return err
			}
		}
		return nil
	}

	if err := walk("/", root); err != nil {
		return nil, err
	}
	return out, nil
}

type gptPartition struct {
	Index uint32
	Name  string
	Start uint64
	Size  uint64
}

func parseGPTPartitions(r io.ReaderAt, sectorSize uint32) ([]gptPartition, error) {
	if sectorSize == 0 {
		sectorSize = 512
	}

	header := make([]byte, sectorSize)
	if _, err := r.ReadAt(header, int64(sectorSize)); err != nil {
		return nil, err
	}
	if string(header[:8]) != "EFI PART" {
		return nil, fmt.Errorf("no GPT header found")
	}

	entriesLBA := binary.LittleEndian.Uint64(header[72:80])
	entryCount := binary.LittleEndian.Uint32(header[80:84])
	entrySize := binary.LittleEndian.Uint32(header[84:88])
	if entrySize < 128 {
		return nil, fmt.Errorf("invalid GPT entry size: %d", entrySize)
	}

	entriesOffset := int64(entriesLBA * uint64(sectorSize))
	entryBuf := make([]byte, entrySize)
	out := make([]gptPartition, 0, entryCount)

	for i := uint32(0); i < entryCount; i++ {
		off := entriesOffset + int64(i)*int64(entrySize)
		if _, err := r.ReadAt(entryBuf, off); err != nil {
			return nil, err
		}
		if bytes.Equal(entryBuf[:16], make([]byte, 16)) {
			continue
		}

		firstLBA := binary.LittleEndian.Uint64(entryBuf[32:40])
		lastLBA := binary.LittleEndian.Uint64(entryBuf[40:48])
		if lastLBA < firstLBA {
			continue
		}

		out = append(out, gptPartition{
			Index: i + 1,
			Name:  decodeUTF16LE(entryBuf[56:128]),
			Start: firstLBA * uint64(sectorSize),
			Size:  (lastLBA - firstLBA + 1) * uint64(sectorSize),
		})
	}

	return out, nil
}

func parseMBRPartitions(r io.ReaderAt, sectorSize uint32) ([]gptPartition, error) {
	if sectorSize == 0 {
		sectorSize = 512
	}

	mbr := make([]byte, 512)
	if _, err := r.ReadAt(mbr, 0); err != nil {
		return nil, err
	}
	if mbr[510] != 0x55 || mbr[511] != 0xAA {
		return nil, fmt.Errorf("no MBR signature found")
	}

	out := make([]gptPartition, 0, 4)
	for i := 0; i < 4; i++ {
		off := 0x1BE + i*16
		ptype := mbr[off+4]
		startLBA := binary.LittleEndian.Uint32(mbr[off+8 : off+12])
		sectors := binary.LittleEndian.Uint32(mbr[off+12 : off+16])
		if ptype == 0 || sectors == 0 {
			continue
		}
		out = append(out, gptPartition{
			Index: uint32(i + 1),
			Name:  mbrPartitionName(ptype),
			Start: uint64(startLBA) * uint64(sectorSize),
			Size:  uint64(sectors) * uint64(sectorSize),
		})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no MBR partitions found")
	}
	return out, nil
}

func mbrPartitionName(t byte) string {
	switch t {
	case 0x07:
		return "Basic data partition"
	case 0x0B, 0x0C, 0x0E:
		return "FAT partition"
	case 0x82:
		return "Linux swap"
	case 0x83:
		return "Linux filesystem"
	case 0xEF:
		return "EFI system partition"
	default:
		return fmt.Sprintf("MBR partition 0x%02x", t)
	}
}

func openVirtualDiskReader(r io.ReaderAt) (io.ReaderAt, uint32, error) {
	vf, err := vhdx.NewVHDXFile(r)
	if err != nil {
		return nil, 0, err
	}
	return vf, uint32(vf.Metadata.LogicalSectorSize), nil
}

type lockedReadSeekerAt struct {
	r  io.ReadSeeker
	mu sync.Mutex
}

func (l *lockedReadSeekerAt) ReadAt(p []byte, off int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.r.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}
	return io.ReadFull(l.r, p)
}

func normalizeGuestPath(p, cwd string) string {
	p = strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	if p == "" {
		if cwd == "" {
			return "/"
		}
		return cwd
	}

	if len(p) >= 2 && p[1] == ':' {
		p = p[2:]
	}
	if !strings.HasPrefix(p, "/") {
		p = joinGuestPath(cwd, p)
	}

	clean := path.Clean(p)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return clean
}

func joinGuestPath(base, name string) string {
	if base == "" || base == "/" {
		return "/" + strings.TrimLeft(name, "/")
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(name, "/")
}

func decodeUTF16LE(buf []byte) string {
	u16 := make([]uint16, 0, len(buf)/2)
	for i := 0; i+1 < len(buf); i += 2 {
		v := binary.LittleEndian.Uint16(buf[i : i+2])
		if v == 0 {
			break
		}
		u16 = append(u16, v)
	}
	return strings.TrimSpace(string(utf16.Decode(u16)))
}

func toUint64NonNegative(v int64) uint64 {
	if v < 0 {
		return 0
	}
	return uint64(v)
}
