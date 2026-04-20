package vbk

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"path"
	"regexp"
	"strconv"
	"strings"

	vhdx "github.com/Velocidex/go-vhdx/parser"
)

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
	vmdkSectorSize  = 512
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
				r:    &boundedReaderAt{r: backing, offset: ex.OffsetSectors * vmdkSectorSize, size: sizeBytes},
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
