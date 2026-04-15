package vbk

import (
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path"
	"strings"
)

var castagnoliTable = crc32.MakeTable(crc32.Castagnoli)

type VBK struct {
	r             io.ReaderAt
	Header        StorageHeader
	FormatVersion uint32
	BlockSize     uint32

	slot1      *SnapshotSlot
	slot2      *SnapshotSlot
	ActiveSlot *SnapshotSlot
	Root       *DirItem

	blockStore *metaVector[StorageBlockDescriptor]
}

func Open(pathname string, verify bool) (*VBK, *os.File, error) {
	f, err := os.Open(pathname)
	if err != nil {
		return nil, nil, err
	}
	v, err := New(f, verify)
	if err != nil {
		_ = f.Close()
		return nil, nil, err
	}
	return v, f, nil
}

func New(r io.ReaderAt, verify bool) (*VBK, error) {
	headerBuf, err := readAt(r, 0, PAGE_SIZE)
	if err != nil {
		return nil, err
	}
	header, err := parseStorageHeader(headerBuf)
	if err != nil {
		return nil, err
	}

	v := &VBK{r: r, Header: header, FormatVersion: header.FormatVersion, BlockSize: header.StandardBlockSize}

	v.slot1, err = newSnapshotSlot(v, PAGE_SIZE)
	if err != nil {
		return nil, err
	}
	v.slot2, err = newSnapshotSlot(v, PAGE_SIZE+v.slot1.Size())
	if err != nil {
		return nil, err
	}

	var populated []*SnapshotSlot
	for _, slot := range []*SnapshotSlot{v.slot1, v.slot2} {
		if slot.Header.ContainsSnapshot == 0 {
			continue
		}
		if verify {
			ok, err := slot.Verify()
			if err != nil || !ok {
				continue
			}
		}
		populated = append(populated, slot)
	}

	if len(populated) == 0 {
		return nil, ErrNoActiveSlot
	}

	v.ActiveSlot = populated[0]
	for _, slot := range populated[1:] {
		if slot.Descriptor.Version > v.ActiveSlot.Descriptor.Version {
			v.ActiveSlot = slot
		}
	}

	v.Root = newRootDirItem(v, v.ActiveSlot.Descriptor.DirectoryRoot.RootPage, v.ActiveSlot.Descriptor.DirectoryRoot.Count)

	stgParser := parseStorageBlockDescriptor
	stgSize := stgBlockDescriptorSize
	if v.IsV7() {
		stgParser = parseStorageBlockDescriptorV7
		stgSize = stgBlockDescriptorV7Size
	}

	v.blockStore, err = newMetaVector(v, stgSize, stgParser, v.ActiveSlot.Descriptor.BlocksStore.RootPage, v.ActiveSlot.Descriptor.BlocksStore.Count)
	if err != nil {
		return nil, err
	}

	return v, nil
}

func (v *VBK) IsV7() bool {
	return v.FormatVersion == 7 || v.FormatVersion == 0x10008 || v.FormatVersion >= 9
}

func (v *VBK) Page(idx int64) ([]byte, error) {
	return v.ActiveSlot.Page(idx)
}

func (v *VBK) MetaBlob(page int64) *MetaBlob {
	return v.ActiveSlot.metaBlob(page)
}

func (v *VBK) Get(p string, base *DirItem) (*DirItem, error) {
	item := base
	if item == nil {
		item = v.Root
	}

	cleaned := strings.TrimSpace(p)
	if cleaned == "" || cleaned == "/" {
		return item, nil
	}

	parts := strings.Split(path.Clean(cleaned), "/")
	for _, part := range parts {
		if part == "" || part == "." {
			continue
		}

		children, err := item.IterDir()
		if err != nil {
			return nil, err
		}

		found := false
		for _, entry := range children {
			if entry.Name == part {
				item = entry
				found = true
				break
			}
		}
		if !found {
			return nil, fmt.Errorf("%w: %s", ErrFileNotFound, p)
		}
	}

	return item, nil
}

type SnapshotSlot struct {
	vbk           *VBK
	offset        int64
	Header        SnapshotSlotHeader
	Descriptor    SnapshotDescriptor
	Grain         BanksGrain
	validMaxBanks uint32
	banks         []Bank
}

func newSnapshotSlot(v *VBK, offset int64) (*SnapshotSlot, error) {
	s := &SnapshotSlot{vbk: v, offset: offset}
	if v.Header.SnapshotSlotFormat == 0 {
		s.validMaxBanks = 0xF8
	} else {
		s.validMaxBanks = 0x7F00
	}

	headBuf, err := readAt(v.r, offset, snapshotSlotHeaderSize)
	if err != nil {
		return nil, err
	}
	s.Header, err = parseSnapshotSlotHeader(headBuf)
	if err != nil {
		return nil, err
	}

	if s.Header.ContainsSnapshot == 0 {
		return s, nil
	}

	descBuf, err := readAt(v.r, offset+snapshotSlotHeaderSize, snapshotDescriptorSize)
	if err != nil {
		return nil, err
	}
	s.Descriptor, err = parseSnapshotDescriptor(descBuf)
	if err != nil {
		return nil, err
	}

	grainBuf, err := readAt(v.r, offset+snapshotSlotHeaderSize+snapshotDescriptorSize, banksGrainSize)
	if err != nil {
		return nil, err
	}
	s.Grain, err = parseBanksGrain(grainBuf)
	if err != nil {
		return nil, err
	}

	if s.Grain.MaxBanks > s.validMaxBanks {
		return nil, fmt.Errorf("%w: invalid snapshot slot max banks", ErrVBK)
	}
	if s.Grain.StoredBanks > s.Grain.MaxBanks {
		return nil, fmt.Errorf("%w: stored banks greater than max banks", ErrVBK)
	}

	bankOffset := offset + snapshotSlotHeaderSize + snapshotDescriptorSize + banksGrainSize
	s.banks = make([]Bank, 0, s.Grain.StoredBanks)
	for i := uint32(0); i < s.Grain.StoredBanks; i++ {
		bdescBuf, err := readAt(v.r, bankOffset+int64(i)*bankDescriptorSize, bankDescriptorSize)
		if err != nil {
			return nil, err
		}
		desc, err := parseBankDescriptor(bdescBuf)
		if err != nil {
			return nil, err
		}
		s.banks = append(s.banks, Bank{vbk: v, Offset: int64(desc.Offset), Size: int64(desc.Size)})
	}

	return s, nil
}

func (s *SnapshotSlot) Size() int64 {
	slotSize := int64(snapshotSlotHeaderSize + snapshotDescriptorSize)
	if s.Header.ContainsSnapshot != 0 {
		slotSize += int64(s.Grain.MaxBanks) * int64(bankDescriptorSize)
	} else {
		slotSize += int64(s.validMaxBanks) * int64(bankDescriptorSize)
	}

	if slotSize&(PAGE_SIZE-1) != 0 {
		slotSize = (slotSize & ^int64(PAGE_SIZE-1)) + PAGE_SIZE
	}
	return slotSize
}

func (s *SnapshotSlot) Verify() (bool, error) {
	if s.Header.ContainsSnapshot == 0 {
		return false, nil
	}

	length := 4 + snapshotDescriptorSize + 8 + int(s.Grain.MaxBanks)*bankDescriptorSize
	buf, err := readAt(s.vbk.r, s.offset+4, length)
	if err != nil {
		return false, err
	}

	var crc uint32
	if s.vbk.Header.SnapshotSlotFormat > 5 {
		crc = crc32.Checksum(buf, castagnoliTable)
	} else {
		crc = crc32.ChecksumIEEE(buf)
	}
	return crc == s.Header.CRC, nil
}

func (s *SnapshotSlot) Page(page int64) ([]byte, error) {
	bankIdx := int(page >> 32)
	if bankIdx < 0 || bankIdx >= len(s.banks) {
		return nil, fmt.Errorf("%w: bank index out of range", ErrVBK)
	}
	return s.banks[bankIdx].Page(page & 0xFFFFFFFF)
}

func (s *SnapshotSlot) metaBlob(page int64) *MetaBlob {
	return &MetaBlob{slot: s, root: page}
}

type Bank struct {
	vbk    *VBK
	Offset int64
	Size   int64
}

func (b *Bank) Verify(crc uint32) (bool, error) {
	buf, err := readAt(b.vbk.r, b.Offset, int(b.Size))
	if err != nil {
		return false, err
	}

	var cur uint32
	if b.vbk.FormatVersion >= 12 && b.vbk.FormatVersion != 0x10008 {
		cur = crc32.Checksum(buf, castagnoliTable)
	} else {
		cur = crc32.ChecksumIEEE(buf)
	}

	return cur == crc, nil
}

func (b *Bank) Page(page int64) ([]byte, error) {
	off := b.Offset + PAGE_SIZE + (page * PAGE_SIZE)
	return readAt(b.vbk.r, off, PAGE_SIZE)
}

func readAt(r io.ReaderAt, off int64, n int) ([]byte, error) {
	buf := make([]byte, n)
	read, err := r.ReadAt(buf, off)
	if err != nil {
		if err == io.EOF && read == n {
			return buf, nil
		}
		return nil, err
	}
	if read != n {
		return nil, io.ErrUnexpectedEOF
	}
	return buf, nil
}
