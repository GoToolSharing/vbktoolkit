package vbk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"sort"
	"strconv"
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

		locked := &lockedReadSeekerAt{r: stream}
		virtualReader, sectorSize, virtualDiskSize, closers, err := openVirtualDiskReader(v, disk, locked)
		g.closers = append(g.closers, closers...)
		if err != nil {
			virtualReader = locked
			sectorSize = inferLogicalSectorSize(disk.Item)
			virtualDiskSize, _ = disk.Item.Size()
		}

		parts, err := parseGPTPartitions(virtualReader, sectorSize)
		if err != nil || len(parts) == 0 {
			parts, err = parseMBRPartitions(virtualReader, sectorSize)
			if err != nil || len(parts) == 0 {
				parts = scanNTFSPartitions(virtualReader, sectorSize, virtualDiskSize)
				if len(parts) == 0 {
					continue
				}
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
			if strings.HasSuffix(low, ".vhd") || strings.HasSuffix(low, ".vhdx") || strings.HasSuffix(low, ".vmdk") {
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

func openVirtualDiskReader(v *VBK, disk guestDiskFile, r io.ReaderAt) (io.ReaderAt, uint32, uint64, []io.Closer, error) {
	vf, err := vhdx.NewVHDXFile(r)
	if err != nil {
		if strings.HasSuffix(strings.ToLower(disk.Path), ".vmdk") {
			vm, size, closers, vmErr := openVMDKReader(v, disk, r)
			if vmErr == nil {
				return vm, 512, size, closers, nil
			}
		}
		return nil, 0, 0, nil, err
	}
	return vf, uint32(vf.Metadata.LogicalSectorSize), uint64(vf.Metadata.VirtualDiskSize), nil, nil
}

const (
	vmdkSectorSize = 512
	vmdkSparseMagic = 0x564d444b
)

type vmdkExtent struct {
	Sectors       uint64
	Type          string
	Path          string
	OffsetSectors uint64
}

type vmdkSparseHeader struct {
	Capacity     uint64
	GrainSize    uint64
	NumGTEsPerGT uint32
	GDOffset     uint64
}

type sizedReaderAt struct {
	r    io.ReaderAt
	size uint64
}

type concatReaderAt struct {
	extents []sizedReaderAt
	offsets []uint64
	size    uint64
}

func newConcatReaderAt(extents []sizedReaderAt) *concatReaderAt {
	c := &concatReaderAt{extents: extents, offsets: make([]uint64, len(extents))}
	running := uint64(0)
	for i, ex := range extents {
		c.offsets[i] = running
		running += ex.size
	}
	c.size = running
	return c
}

func (c *concatReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off) >= c.size {
		return 0, io.EOF
	}

	read := 0
	cur := uint64(off)
	for read < len(p) && cur < c.size {
		i := c.extentIndex(cur)
		rel := cur - c.offsets[i]
		avail := c.extents[i].size - rel
		want := minInt(len(p)-read, int(avail))
		n, err := c.extents[i].r.ReadAt(p[read:read+want], int64(rel))
		read += n
		cur += uint64(n)
		if err != nil && !errors.Is(err, io.EOF) {
			return read, err
		}
		if n < want {
			break
		}
	}

	if read < len(p) {
		return read, io.EOF
	}
	return read, nil
}

func (c *concatReaderAt) extentIndex(off uint64) int {
	for i := len(c.offsets) - 1; i >= 0; i-- {
		if off >= c.offsets[i] {
			return i
		}
	}
	return 0
}

type boundedReaderAt struct {
	r      io.ReaderAt
	offset uint64
	size   uint64
}

func (b *boundedReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off) >= b.size {
		return 0, io.EOF
	}

	max := int(b.size - uint64(off))
	want := minInt(len(p), max)
	n, err := b.r.ReadAt(p[:want], int64(b.offset+uint64(off)))
	if n < want && err == nil {
		err = io.EOF
	}
	if n == len(p) {
		return n, err
	}
	if n < len(p) {
		return n, io.EOF
	}
	return n, err
}

type zeroReaderAt struct{}

func (z *zeroReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	for i := range p {
		p[i] = 0
	}
	return len(p), nil
}

type vmdkSparseReader struct {
	r      io.ReaderAt
	header vmdkSparseHeader
	gd     []uint32
}

func (r *vmdkSparseReader) Size() uint64 {
	return r.header.Capacity * vmdkSectorSize
}

func (r *vmdkSparseReader) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 {
		return 0, fmt.Errorf("negative offset")
	}
	if len(p) == 0 {
		return 0, nil
	}
	if uint64(off) >= r.Size() {
		return 0, io.EOF
	}

	read := 0
	cur := uint64(off)
	limit := r.Size()
	sectorBuf := make([]byte, vmdkSectorSize)

	for read < len(p) && cur < limit {
		sector := cur / vmdkSectorSize
		sectorOff := int(cur % vmdkSectorSize)
		maxChunk := minInt(len(p)-read, vmdkSectorSize-sectorOff)
		remaining := int(limit - cur)
		chunk := minInt(maxChunk, remaining)

		if err := r.readSector(sector, sectorBuf); err != nil {
			return read, err
		}
		copy(p[read:read+chunk], sectorBuf[sectorOff:sectorOff+chunk])
		read += chunk
		cur += uint64(chunk)
	}

	if read < len(p) {
		return read, io.EOF
	}
	return read, nil
}

func (r *vmdkSparseReader) readSector(sector uint64, dst []byte) error {
	for i := range dst {
		dst[i] = 0
	}

	gtCoverage := r.header.GrainSize * uint64(r.header.NumGTEsPerGT)
	if gtCoverage == 0 {
		return fmt.Errorf("invalid VMDK sparse grain table coverage")
	}

	gdIndex := sector / gtCoverage
	if gdIndex >= uint64(len(r.gd)) {
		return nil
	}
	gtSector := r.gd[gdIndex]
	if gtSector == 0 {
		return nil
	}

	gteIndex := (sector / r.header.GrainSize) % uint64(r.header.NumGTEsPerGT)
	gteBuf := make([]byte, 4)
	gtOff := uint64(gtSector)*vmdkSectorSize + gteIndex*4
	if _, err := r.r.ReadAt(gteBuf, int64(gtOff)); err != nil {
		return err
	}
	grainSector := binary.LittleEndian.Uint32(gteBuf)
	if grainSector == 0 {
		return nil
	}

	grainSectorOffset := sector % r.header.GrainSize
	dataOff := (uint64(grainSector) + grainSectorOffset) * vmdkSectorSize
	_, err := r.r.ReadAt(dst, int64(dataOff))
	if errors.Is(err, io.EOF) {
		return nil
	}
	return err
}

func openVMDKReader(v *VBK, disk guestDiskFile, primary io.ReaderAt) (io.ReaderAt, uint64, []io.Closer, error) {
	if sparse, err := openSparseVMDKReader(primary); err == nil {
		return sparse, sparse.Size(), nil, nil
	}

	descriptor, err := readVMDKDescriptor(primary)
	if err != nil {
		return nil, 0, nil, err
	}
	extents, err := parseVMDKExtents(descriptor)
	if err != nil {
		return nil, 0, nil, err
	}

	closers := make([]io.Closer, 0, len(extents))
	readers := make([]sizedReaderAt, 0, len(extents))
	baseDir := path.Dir(disk.Path)

	for _, ex := range extents {
		sizeBytes := ex.Sectors * vmdkSectorSize
		if sizeBytes == 0 {
			continue
		}

		upperType := strings.ToUpper(ex.Type)
		if ex.Path == "" {
			if upperType == "ZERO" {
				readers = append(readers, sizedReaderAt{r: &zeroReaderAt{}, size: sizeBytes})
				continue
			}
			return nil, 0, nil, fmt.Errorf("unsupported VMDK extent without path: %s", ex.Type)
		}

		resolved := resolveVMDKPath(baseDir, ex.Path)
		item, err := v.Get(resolved, nil)
		if err != nil {
			return nil, 0, nil, err
		}
		stream, err := item.Open()
		if err != nil {
			return nil, 0, nil, err
		}
		closers = append(closers, stream)

		backing := io.ReaderAt(&lockedReadSeekerAt{r: stream})
		switch upperType {
		case "FLAT", "VMFS", "RAW":
			readers = append(readers, sizedReaderAt{
				r: &boundedReaderAt{r: backing, offset: ex.OffsetSectors * vmdkSectorSize, size: sizeBytes},
				size: sizeBytes,
			})
		case "SPARSE", "VMFSSPARSE":
			sparse, err := openSparseVMDKReader(backing)
			if err != nil {
				return nil, 0, nil, err
			}
			readers = append(readers, sizedReaderAt{
				r:    &boundedReaderAt{r: sparse, offset: 0, size: minUint64(sizeBytes, sparse.Size())},
				size: minUint64(sizeBytes, sparse.Size()),
			})
		default:
			return nil, 0, nil, fmt.Errorf("unsupported VMDK extent type: %s", ex.Type)
		}
	}

	if len(readers) == 0 {
		return nil, 0, nil, fmt.Errorf("no readable extents in VMDK descriptor")
	}
	if len(readers) == 1 {
		return readers[0].r, readers[0].size, closers, nil
	}

	concat := newConcatReaderAt(readers)
	return concat, concat.size, closers, nil
}

func openSparseVMDKReader(r io.ReaderAt) (*vmdkSparseReader, error) {
	head := make([]byte, vmdkSectorSize)
	if _, err := r.ReadAt(head, 0); err != nil {
		return nil, err
	}
	if binary.LittleEndian.Uint32(head[0:4]) != vmdkSparseMagic {
		return nil, fmt.Errorf("not a sparse VMDK")
	}

	hdr := vmdkSparseHeader{
		Capacity:     binary.LittleEndian.Uint64(head[12:20]),
		GrainSize:    binary.LittleEndian.Uint64(head[20:28]),
		NumGTEsPerGT: binary.LittleEndian.Uint32(head[44:48]),
		GDOffset:     binary.LittleEndian.Uint64(head[56:64]),
	}
	if hdr.Capacity == 0 || hdr.GrainSize == 0 || hdr.NumGTEsPerGT == 0 || hdr.GDOffset == 0 {
		return nil, fmt.Errorf("invalid sparse VMDK header")
	}

	gdEntries := (hdr.Capacity + hdr.GrainSize*uint64(hdr.NumGTEsPerGT) - 1) / (hdr.GrainSize * uint64(hdr.NumGTEsPerGT))
	gd := make([]uint32, gdEntries)
	gdBuf := make([]byte, gdEntries*4)
	if _, err := r.ReadAt(gdBuf, int64(hdr.GDOffset*vmdkSectorSize)); err != nil {
		return nil, err
	}
	for i := uint64(0); i < gdEntries; i++ {
		gd[i] = binary.LittleEndian.Uint32(gdBuf[i*4 : i*4+4])
	}

	return &vmdkSparseReader{r: r, header: hdr, gd: gd}, nil
}

func readVMDKDescriptor(r io.ReaderAt) (string, error) {
	const maxDescriptorProbe = 1 << 20
	buf := make([]byte, maxDescriptorProbe)
	n, err := r.ReadAt(buf, 0)
	if err != nil && !errors.Is(err, io.EOF) {
		return "", err
	}
	if n == 0 {
		return "", fmt.Errorf("empty VMDK descriptor")
	}
	buf = buf[:n]
	if idx := bytes.IndexByte(buf, 0); idx >= 0 {
		buf = buf[:idx]
	}

	text := string(buf)
	lower := strings.ToLower(text)
	if !strings.Contains(lower, "disk descriptorfile") && !strings.Contains(lower, "ddb.") {
		return "", fmt.Errorf("not a VMDK descriptor")
	}
	return text, nil
}

var vmdkExtentLineRe = regexp.MustCompile(`(?i)^\s*RW\s+(\d+)\s+([A-Z0-9_]+)(?:\s+"([^"]+)")?(?:\s+(\d+))?\s*$`)

func parseVMDKExtents(descriptor string) ([]vmdkExtent, error) {
	out := make([]vmdkExtent, 0, 4)
	lines := strings.Split(descriptor, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		m := vmdkExtentLineRe.FindStringSubmatch(line)
		if m == nil {
			continue
		}

		sectors, err := strconv.ParseUint(m[1], 10, 64)
		if err != nil {
			return nil, err
		}
		offSectors := uint64(0)
		if m[4] != "" {
			offSectors, err = strconv.ParseUint(m[4], 10, 64)
			if err != nil {
				return nil, err
			}
		}

		out = append(out, vmdkExtent{
			Sectors:       sectors,
			Type:          strings.ToUpper(m[2]),
			Path:          strings.TrimSpace(m[3]),
			OffsetSectors: offSectors,
		})
	}

	if len(out) == 0 {
		return nil, fmt.Errorf("no VMDK extents found in descriptor")
	}
	return out, nil
}

func resolveVMDKPath(base, ref string) string {
	ref = strings.ReplaceAll(strings.TrimSpace(ref), "\\", "/")
	if strings.HasPrefix(ref, "/") {
		return path.Clean(ref)
	}
	p := path.Join(base, ref)
	if !strings.HasPrefix(p, "/") {
		p = "/" + p
	}
	return path.Clean(p)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func minUint64(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func inferLogicalSectorSize(item *DirItem) uint32 {
	props, err := item.Properties()
	if err == nil && props != nil {
		if raw, ok := props["LogicalSectorSize"]; ok {
			switch v := raw.(type) {
			case uint32:
				if v > 0 {
					return v
				}
			case uint64:
				if v > 0 {
					return uint32(v)
				}
			case int:
				if v > 0 {
					return uint32(v)
				}
			}
		}
	}
	return 512
}

func scanNTFSPartitions(r io.ReaderAt, sectorSize uint32, diskSize uint64) []gptPartition {
	if sectorSize == 0 {
		sectorSize = 512
	}
	if diskSize == 0 {
		return nil
	}

	const scanCap = 2 * 1024 * 1024 * 1024 // 2 GiB
	limit := diskSize
	if limit > scanCap {
		limit = scanCap
	}

	buf := make([]byte, sectorSize)
	seen := map[uint64]struct{}{}
	out := make([]gptPartition, 0, 4)

	for off := uint64(0); off+uint64(sectorSize) <= limit; off += 1024 * 1024 {
		if _, err := r.ReadAt(buf, int64(off)); err != nil {
			continue
		}
		if !looksLikeNTFSBootSector(buf) {
			continue
		}
		if _, ok := seen[off]; ok {
			continue
		}
		seen[off] = struct{}{}
		out = append(out, gptPartition{
			Index: uint32(len(out) + 1),
			Name:  "Basic data partition",
			Start: off,
			Size:  diskSize - off,
		})
	}

	return out
}

func looksLikeNTFSBootSector(buf []byte) bool {
	if len(buf) < 512 {
		return false
	}
	if buf[510] != 0x55 || buf[511] != 0xAA {
		return false
	}
	return bytes.Equal(buf[3:11], []byte("NTFS    "))
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

	read := 0
	for read < len(p) {
		n, err := l.r.Read(p[read:])
		read += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				if read == len(p) {
					return read, nil
				}
				return read, io.EOF
			}
			return read, err
		}
		if n == 0 {
			break
		}
	}

	if read < len(p) {
		return read, io.EOF
	}
	return read, nil
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
