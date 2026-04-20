package vbk

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"strings"
	"unicode/utf16"
)

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

	const scanCap = 2 * 1024 * 1024 * 1024
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
