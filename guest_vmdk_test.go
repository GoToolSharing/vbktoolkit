package vbk

import (
	"encoding/binary"
	"io"
	"testing"
)

func TestParseVMDKExtents(t *testing.T) {
	descriptor := `
# Disk DescriptorFile
RW 2048 FLAT "disk-flat.vmdk" 0
RW 4096 SPARSE "disk-s001.vmdk"
`

	extents, err := parseVMDKExtents(descriptor)
	if err != nil {
		t.Fatalf("parseVMDKExtents failed: %v", err)
	}
	if len(extents) != 2 {
		t.Fatalf("expected 2 extents, got %d", len(extents))
	}
	if extents[0].Type != "FLAT" || extents[0].Path != "disk-flat.vmdk" || extents[0].OffsetSectors != 0 {
		t.Fatalf("unexpected first extent: %+v", extents[0])
	}
	if extents[1].Type != "SPARSE" || extents[1].Path != "disk-s001.vmdk" {
		t.Fatalf("unexpected second extent: %+v", extents[1])
	}
}

func TestOpenSparseVMDKReader(t *testing.T) {
	const sectors = 16
	const grainSize = 8

	buf := make([]byte, 12*vmdkSectorSize)
	binary.LittleEndian.PutUint32(buf[0:4], vmdkSparseMagic)
	binary.LittleEndian.PutUint64(buf[12:20], sectors)
	binary.LittleEndian.PutUint64(buf[20:28], grainSize)
	binary.LittleEndian.PutUint32(buf[44:48], 512)
	binary.LittleEndian.PutUint64(buf[56:64], 2)

	binary.LittleEndian.PutUint32(buf[2*vmdkSectorSize:2*vmdkSectorSize+4], 3)
	binary.LittleEndian.PutUint32(buf[3*vmdkSectorSize:3*vmdkSectorSize+4], 4)

	for i := 0; i < vmdkSectorSize; i++ {
		buf[4*vmdkSectorSize+i] = byte(i % 251)
	}

	r, err := openSparseVMDKReader(bytesReaderAt(buf))
	if err != nil {
		t.Fatalf("openSparseVMDKReader failed: %v", err)
	}

	block := make([]byte, vmdkSectorSize)
	if _, err := r.ReadAt(block, 0); err != nil {
		t.Fatalf("read first sector failed: %v", err)
	}
	for i := 0; i < vmdkSectorSize; i++ {
		if block[i] != byte(i%251) {
			t.Fatalf("unexpected first grain byte at %d: got=%d", i, block[i])
		}
	}

	if _, err := r.ReadAt(block, int64(8*vmdkSectorSize)); err != nil {
		t.Fatalf("read sparse sector failed: %v", err)
	}
	for i := 0; i < vmdkSectorSize; i++ {
		if block[i] != 0 {
			t.Fatalf("expected sparse zero byte at %d, got=%d", i, block[i])
		}
	}
}

type bytesReaderAt []byte

func (b bytesReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off < 0 || off >= int64(len(b)) {
		return 0, io.EOF
	}
	n := copy(p, b[int(off):])
	if n < len(p) {
		return n, io.EOF
	}
	return n, nil
}
