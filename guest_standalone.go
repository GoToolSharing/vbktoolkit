package vbk

import (
	"fmt"
	"io"
	"os"
	"strings"

	vhdx "github.com/Velocidex/go-vhdx/parser"
)

// OpenDisk opens a standalone virtual disk image from the local filesystem and
// discovers its guest volumes, exactly as (*VBK).DiscoverGuest does for disks
// embedded inside a VBK. Use it when you already have the disk (for example a
// .vhdx exported on its own) and no surrounding .vbk backup.
//
// VHDX images are parsed natively. A single self-contained VMDK (monolithic
// sparse) file is also supported. Any other image — raw/flat disk dumps and
// fixed VHD — is treated as a raw disk and scanned directly for its partition
// table. Dynamic VHD and multi-extent VMDK layouts are not supported here.
//
// The returned Guest owns the underlying file handle; call Close when done.
func OpenDisk(diskPath string) (*Guest, error) {
	f, err := os.Open(diskPath)
	if err != nil {
		return nil, err
	}

	g := &Guest{volumes: make([]*GuestVolume, 0, 4)}
	g.closers = append(g.closers, f)

	virtualReader, sectorSize, virtualDiskSize, closers := openStandaloneDiskReader(diskPath, f)
	g.closers = append(g.closers, closers...)

	g.addVolumesFromDisk(diskPath, virtualReader, sectorSize, virtualDiskSize)
	if len(g.volumes) == 0 {
		_ = g.Close()
		return nil, fmt.Errorf("no readable guest volume found in %s", diskPath)
	}

	g.pickDefaultVolume()
	return g, nil
}

// openStandaloneDiskReader turns a local disk image file into a raw io.ReaderAt
// over its byte content, along with the logical sector size and virtual disk
// size needed for partition parsing. Unlike the VBK path there is no seekable
// stream to wrap: *os.File already provides concurrency-safe ReadAt.
func openStandaloneDiskReader(diskPath string, f *os.File) (io.ReaderAt, uint32, uint64, []io.Closer) {
	if vf, err := vhdx.NewVHDXFile(f); err == nil {
		return vf, uint32(vf.Metadata.LogicalSectorSize), uint64(vf.Metadata.VirtualDiskSize), nil
	}

	if strings.HasSuffix(strings.ToLower(diskPath), ".vmdk") {
		if sparse, err := openSparseVMDKReader(f); err == nil {
			return sparse, vmdkSectorSize, sparse.Size(), nil
		}
	}

	// Raw / flat / fixed-VHD fallback: read the file as-is.
	size := uint64(0)
	if st, err := f.Stat(); err == nil && st.Size() > 0 {
		size = uint64(st.Size())
	}
	return f, 512, size, nil
}
