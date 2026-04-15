package vbk

import (
	"encoding/binary"
	"fmt"
)

const (
	PAGE_SIZE = 4096

	snapshotSlotHeaderSize = 8
	snapshotDescriptorSize = 108
	banksGrainSize         = 8
	bankDescriptorSize     = 16
	metaBlobHeaderSize     = 12
	dirItemRecordSize      = 0xC0

	fibBlockDescriptorSize   = 30
	fibBlockDescriptorV7Size = 46
	stgBlockDescriptorSize   = 44
	stgBlockDescriptorV7Size = 60
	metaTableDescriptorSize  = 24
)

type DirItemType uint32

const (
	DirItemNone      DirItemType = 0
	DirItemSubFolder DirItemType = 1
	DirItemExtFib    DirItemType = 2
	DirItemIntFib    DirItemType = 3
	DirItemPatch     DirItemType = 4
	DirItemIncrement DirItemType = 5
)

type BlockLocationType uint8

const (
	BlockNormal         BlockLocationType = 0x00
	BlockSparse         BlockLocationType = 0x01
	BlockReserved       BlockLocationType = 0x02
	BlockArchived       BlockLocationType = 0x03
	BlockInBlob         BlockLocationType = 0x04
	BlockInBlobReserved BlockLocationType = 0x05
)

type CompressionType int8

const (
	CompressionPlain CompressionType = -1
	CompressionRL    CompressionType = 2
	CompressionZLH   CompressionType = 3
	CompressionZLL   CompressionType = 4
	CompressionLZ4   CompressionType = 7
)

type PropertyType int32

const (
	PropertyUInt32  PropertyType = 1
	PropertyUInt64  PropertyType = 2
	PropertyAString PropertyType = 3
	PropertyWString PropertyType = 4
	PropertyBinary  PropertyType = 5
	PropertyBoolean PropertyType = 6
	PropertyEnd     PropertyType = -1
)

type StorageHeader struct {
	FormatVersion      uint32
	Initialized        uint32
	DigestTypeLength   uint32
	DigestType         [251]byte
	SnapshotSlotFormat uint32
	StandardBlockSize  uint32
	ClusterAlign       uint8
	ExternalStorageID  [16]byte
}

func parseStorageHeader(buf []byte) (StorageHeader, error) {
	if len(buf) < 0x140 {
		return StorageHeader{}, fmt.Errorf("%w: storage header too small", ErrVBK)
	}

	var h StorageHeader
	h.FormatVersion = leU32(buf, 0x00)
	h.Initialized = leU32(buf, 0x04)
	h.DigestTypeLength = leU32(buf, 0x08)
	copy(h.DigestType[:], buf[0x0C:0x107])
	h.SnapshotSlotFormat = leU32(buf, 0x107)
	h.StandardBlockSize = leU32(buf, 0x10B)
	h.ClusterAlign = buf[0x10F]
	copy(h.ExternalStorageID[:], buf[0x130:0x140])
	return h, nil
}

type SnapshotSlotHeader struct {
	CRC              uint32
	ContainsSnapshot uint32
}

func parseSnapshotSlotHeader(buf []byte) (SnapshotSlotHeader, error) {
	if len(buf) < snapshotSlotHeaderSize {
		return SnapshotSlotHeader{}, fmt.Errorf("%w: snapshot slot header too small", ErrVBK)
	}
	return SnapshotSlotHeader{
		CRC:              leU32(buf, 0),
		ContainsSnapshot: leU32(buf, 4),
	}, nil
}

type DirectoryRootRecord struct {
	RootPage int64
	Count    uint64
}

type BlocksStoreHeader struct {
	RootPage              int64
	Count                 uint64
	FreeRootPage          int64
	DeduplicationRootPage int64
	Unk0                  int64
	Unk1                  int64
}

type CryptoStoreRecord struct {
	RootPage int64
}

type SnapshotDescriptor struct {
	Version       uint64
	StorageEOF    uint64
	BanksCount    uint32
	DirectoryRoot DirectoryRootRecord
	BlocksStore   BlocksStoreHeader
	CryptoStore   CryptoStoreRecord
	Unk0          uint64
	Unk1          uint64
}

func parseSnapshotDescriptor(buf []byte) (SnapshotDescriptor, error) {
	if len(buf) < snapshotDescriptorSize {
		return SnapshotDescriptor{}, fmt.Errorf("%w: snapshot descriptor too small", ErrVBK)
	}

	return SnapshotDescriptor{
		Version:    leU64(buf, 0),
		StorageEOF: leU64(buf, 8),
		BanksCount: leU32(buf, 16),
		DirectoryRoot: DirectoryRootRecord{
			RootPage: leI64(buf, 20),
			Count:    leU64(buf, 28),
		},
		BlocksStore: BlocksStoreHeader{
			RootPage:              leI64(buf, 36),
			Count:                 leU64(buf, 44),
			FreeRootPage:          leI64(buf, 52),
			DeduplicationRootPage: leI64(buf, 60),
			Unk0:                  leI64(buf, 68),
			Unk1:                  leI64(buf, 76),
		},
		CryptoStore: CryptoStoreRecord{RootPage: leI64(buf, 84)},
		Unk0:        leU64(buf, 92),
		Unk1:        leU64(buf, 100),
	}, nil
}

type BanksGrain struct {
	MaxBanks    uint32
	StoredBanks uint32
}

func parseBanksGrain(buf []byte) (BanksGrain, error) {
	if len(buf) < banksGrainSize {
		return BanksGrain{}, fmt.Errorf("%w: banks grain too small", ErrVBK)
	}
	return BanksGrain{MaxBanks: leU32(buf, 0), StoredBanks: leU32(buf, 4)}, nil
}

type BankDescriptor struct {
	CRC    uint32
	Offset uint64
	Size   uint32
}

func parseBankDescriptor(buf []byte) (BankDescriptor, error) {
	if len(buf) < bankDescriptorSize {
		return BankDescriptor{}, fmt.Errorf("%w: bank descriptor too small", ErrVBK)
	}
	return BankDescriptor{CRC: leU32(buf, 0), Offset: leU64(buf, 4), Size: leU32(buf, 12)}, nil
}

type DirItemRecord struct {
	Type          DirItemType
	NameLength    uint32
	Name          [128]byte
	PropsRootPage int64
	Unk1          uint32
	Data          [44]byte
}

func parseDirItemRecord(buf []byte) (DirItemRecord, error) {
	if len(buf) < dirItemRecordSize {
		return DirItemRecord{}, fmt.Errorf("%w: dir item record too small", ErrVBK)
	}
	var rec DirItemRecord
	rec.Type = DirItemType(leU32(buf, 0))
	rec.NameLength = leU32(buf, 4)
	copy(rec.Name[:], buf[8:136])
	rec.PropsRootPage = leI64(buf, 136)
	rec.Unk1 = leU32(buf, 144)
	copy(rec.Data[:], buf[148:192])
	return rec, nil
}

func (r DirItemRecord) subFolderRoot() int64   { return int64(leU64(r.Data[:], 0)) }
func (r DirItemRecord) subFolderCount() uint64 { return uint64(leU32(r.Data[:], 8)) }

func (r DirItemRecord) blocksVectorRoot() int64   { return int64(leU64(r.Data[:], 4)) }
func (r DirItemRecord) blocksVectorCount() uint64 { return leU64(r.Data[:], 12) }
func (r DirItemRecord) fibSize() uint64           { return leU64(r.Data[:], 20) }

type MetaTableDescriptor struct {
	RootPage  int64
	BlockSize uint64
	Count     uint64
}

func parseMetaTableDescriptor(_ *VBK, buf []byte) (MetaTableDescriptor, error) {
	if len(buf) < metaTableDescriptorSize {
		return MetaTableDescriptor{}, fmt.Errorf("%w: meta table descriptor too small", ErrVBK)
	}
	return MetaTableDescriptor{
		RootPage:  leI64(buf, 0),
		BlockSize: leU64(buf, 8),
		Count:     leU64(buf, 16),
	}, nil
}

type FibBlockDescriptor struct {
	BlockSize uint32
	Type      BlockLocationType
	Digest    [16]byte
	BlockID   uint64
	Flags     uint8
	KeySetID  []byte
}

func parseFibBlockDescriptor(_ *VBK, buf []byte) (FibBlockDescriptor, error) {
	if len(buf) < fibBlockDescriptorSize {
		return FibBlockDescriptor{}, fmt.Errorf("%w: fib block descriptor too small", ErrVBK)
	}
	var out FibBlockDescriptor
	out.BlockSize = leU32(buf, 0)
	out.Type = BlockLocationType(buf[4])
	copy(out.Digest[:], buf[5:21])
	out.BlockID = leU64(buf, 21)
	out.Flags = buf[29]
	return out, nil
}

func parseFibBlockDescriptorV7(_ *VBK, buf []byte) (FibBlockDescriptor, error) {
	if len(buf) < fibBlockDescriptorV7Size {
		return FibBlockDescriptor{}, fmt.Errorf("%w: fib block descriptor v7 too small", ErrVBK)
	}
	out, err := parseFibBlockDescriptor(nil, buf[:fibBlockDescriptorSize])
	if err != nil {
		return FibBlockDescriptor{}, err
	}
	out.KeySetID = append([]byte(nil), buf[30:46]...)
	return out, nil
}

func (d FibBlockDescriptor) IsNormal() bool       { return d.Type == BlockNormal }
func (d FibBlockDescriptor) IsSparse() bool       { return d.Type == BlockSparse }
func (d FibBlockDescriptor) IsReserved() bool     { return d.Type == BlockReserved }
func (d FibBlockDescriptor) IsArchived() bool     { return d.Type == BlockArchived }
func (d FibBlockDescriptor) IsBlockInBlob() bool  { return d.Type == BlockInBlob }
func (d FibBlockDescriptor) IsBlobReserved() bool { return d.Type == BlockInBlobReserved }

type StorageBlockDescriptor struct {
	Format          uint8
	UsageCounter    uint32
	Offset          uint64
	AllocatedSize   uint32
	Deduplication   uint8
	Digest          [16]byte
	CompressionType CompressionType
	CompressedSize  uint32
	SourceSize      uint32
	KeySetID        []byte
}

func parseStorageBlockDescriptor(_ *VBK, buf []byte) (StorageBlockDescriptor, error) {
	if len(buf) < stgBlockDescriptorSize {
		return StorageBlockDescriptor{}, fmt.Errorf("%w: storage block descriptor too small", ErrVBK)
	}
	var d StorageBlockDescriptor
	d.Format = buf[0]
	d.UsageCounter = leU32(buf, 1)
	d.Offset = leU64(buf, 5)
	d.AllocatedSize = leU32(buf, 13)
	d.Deduplication = buf[17]
	copy(d.Digest[:], buf[18:34])
	d.CompressionType = CompressionType(int8(buf[34]))
	d.CompressedSize = leU32(buf, 36)
	d.SourceSize = leU32(buf, 40)
	return d, nil
}

func parseStorageBlockDescriptorV7(_ *VBK, buf []byte) (StorageBlockDescriptor, error) {
	if len(buf) < stgBlockDescriptorV7Size {
		return StorageBlockDescriptor{}, fmt.Errorf("%w: storage block descriptor v7 too small", ErrVBK)
	}
	d, err := parseStorageBlockDescriptor(nil, buf[:stgBlockDescriptorSize])
	if err != nil {
		return StorageBlockDescriptor{}, err
	}
	d.KeySetID = append([]byte(nil), buf[44:60]...)
	return d, nil
}

func (d StorageBlockDescriptor) IsLegacy() bool     { return d.Format != 4 }
func (d StorageBlockDescriptor) IsDataBlock() bool  { return d.UsageCounter != 0 }
func (d StorageBlockDescriptor) IsDedupBlock() bool { return d.Deduplication != 0 }
func (d StorageBlockDescriptor) IsCompressed() bool { return d.CompressionType != CompressionPlain }

func leU32(buf []byte, off int) uint32 {
	return binary.LittleEndian.Uint32(buf[off : off+4])
}

func leI32(buf []byte, off int) int32 {
	return int32(leU32(buf, off))
}

func leU64(buf []byte, off int) uint64 {
	return binary.LittleEndian.Uint64(buf[off : off+8])
}

func leI64(buf []byte, off int) int64 {
	return int64(leU64(buf, off))
}
