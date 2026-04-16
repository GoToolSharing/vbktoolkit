package vbk

import (
	"io"
	"math"

	ext4fs "github.com/diskfs/go-diskfs/filesystem/ext4"
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
	ext4    *ext4fs.FileSystem
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
			} else if p.Size > 0 && p.Size <= uint64(math.MaxInt64) {
				ext4Reader := &boundedReaderAt{r: virtualReader, offset: p.Start, size: p.Size}
				storage := &readerAtStorage{r: ext4Reader, size: int64(p.Size)}
				ext, extErr := ext4fs.Read(storage, int64(p.Size), 0, 512)
				if extErr == nil {
					vol.fsType = "ext4"
					vol.ext4 = ext
				}
			}

			if vol.Name == "" {
				if vol.fsType == "ntfs" || vol.fsType == "ext4" {
					vol.Name = "Basic data partition"
				} else {
					vol.Name = "Partition"
				}
			}

			g.volumes = append(g.volumes, vol)
		}
	}

	for i, vol := range g.volumes {
		if vol.fsType == "ntfs" {
			if vol.PathExists("/Windows") || vol.PathExists("/Users") {
				g.defaultIndex = i
				break
			}
			continue
		}
		if vol.fsType == "ext4" {
			if vol.PathExists("/etc") || vol.PathExists("/root") || vol.PathExists("/home") {
				g.defaultIndex = i
				break
			}
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
