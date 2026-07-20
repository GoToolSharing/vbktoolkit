package vbk

import (
	"io"
	"math"

	ext4 "github.com/Velocidex/go-ext4/parser"
	xfsfs "github.com/masahiro331/go-xfs-filesystem/xfs"
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
	ext4Ctx *ext4.EXT4Context
	xfsFS   *xfsfs.FileSystem
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

		g.addVolumesFromDisk(disk.Path, virtualReader, sectorSize, virtualDiskSize)
	}

	g.pickDefaultVolume()
	return g, nil
}

// addVolumesFromDisk parses the partition layout of a single virtual disk
// (already exposed as a raw io.ReaderAt over the disk's byte content) and
// appends one GuestVolume per detected filesystem partition. It is shared by
// DiscoverGuest (disks embedded in a VBK) and OpenDisk (standalone images).
func (g *Guest) addVolumesFromDisk(diskPath string, virtualReader io.ReaderAt, sectorSize uint32, virtualDiskSize uint64) {
	parts, err := parseGPTPartitions(virtualReader, sectorSize)
	if err != nil || len(parts) == 0 {
		parts, err = parseMBRPartitions(virtualReader, sectorSize)
		if err != nil || len(parts) == 0 {
			parts = scanNTFSPartitions(virtualReader, sectorSize, virtualDiskSize)
			if len(parts) == 0 {
				return
			}
		}
	}

	for _, p := range parts {
		vol := &GuestVolume{
			Index:       len(g.volumes),
			DiskPath:    diskPath,
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
		} else if p.Size > 0 && p.Size <= uint64(math.MaxInt64) {
			ext4Reader := &boundedReaderAt{r: virtualReader, offset: p.Start, size: p.Size}
			ext, extErr := ext4.GetEXT4Context(ext4Reader)
			if extErr == nil {
				vol.fsType = "ext"
				vol.ext4Ctx = ext
			} else {
				xfsReader := io.NewSectionReader(ext4Reader, 0, int64(p.Size))
				xfs, xfsErr := xfsfs.NewFS(*xfsReader, nil)
				if xfsErr == nil {
					vol.fsType = "xfs"
					vol.xfsFS = xfs
				}
			}
		}

		if vol.Name == "" {
			if vol.fsType == "ntfs" || vol.fsType == "ext" || vol.fsType == "xfs" {
				vol.Name = "Basic data partition"
			} else {
				vol.Name = "Partition"
			}
		}

		g.volumes = append(g.volumes, vol)
	}
}

// pickDefaultVolume selects the volume that most likely holds the guest OS
// (a Windows/Linux system root) as the default active volume, leaving
// defaultIndex at 0 when no heuristic matches.
func (g *Guest) pickDefaultVolume() {
	for i, vol := range g.volumes {
		switch vol.fsType {
		case "ntfs":
			if vol.PathExists("/Windows") || vol.PathExists("/Users") {
				g.defaultIndex = i
				return
			}
		case "ext", "xfs":
			if vol.PathExists("/etc") || vol.PathExists("/root") || vol.PathExists("/home") {
				g.defaultIndex = i
				return
			}
		}
	}
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
