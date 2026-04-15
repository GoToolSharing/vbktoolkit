package vbk

import (
	"fmt"
	"io"

	"github.com/pierrec/lz4/v4"
)

type FibStream struct {
	vbk       *VBK
	table     *FibMetaSparseTable
	size      int64
	blockSize int64
	pos       int64
}

func newFibStream(vbk *VBK, page int64, count uint64, size int64) (*FibStream, error) {
	table, err := newFibMetaSparseTable(vbk, page, count)
	if err != nil {
		return nil, err
	}
	return &FibStream{vbk: vbk, table: table, size: size, blockSize: int64(vbk.BlockSize)}, nil
}

func (s *FibStream) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if s.pos >= s.size {
		return 0, io.EOF
	}
	toRead := int64(len(p))
	if s.pos+toRead > s.size {
		toRead = s.size - s.pos
	}

	buf, err := s.readRange(s.pos, toRead)
	if err != nil {
		return 0, err
	}
	n := copy(p, buf)
	s.pos += int64(n)
	if int64(n) < int64(len(p)) {
		return n, io.EOF
	}
	return n, nil
}

func (s *FibStream) Seek(offset int64, whence int) (int64, error) {
	var next int64
	switch whence {
	case io.SeekStart:
		next = offset
	case io.SeekCurrent:
		next = s.pos + offset
	case io.SeekEnd:
		next = s.size + offset
	default:
		return 0, fmt.Errorf("%w: invalid whence", ErrVBK)
	}
	if next < 0 {
		return 0, fmt.Errorf("%w: negative seek", ErrVBK)
	}
	s.pos = next
	return s.pos, nil
}

func (s *FibStream) Close() error {
	return nil
}

func (s *FibStream) readRange(offset int64, length int64) ([]byte, error) {
	result := make([]byte, 0, length)

	for length > 0 {
		blockIdx := uint64(offset / s.blockSize)
		offsetInBlock := offset % s.blockSize
		readSize := min64(length, s.blockSize-offsetInBlock)

		blockDesc, err := s.table.get(blockIdx)
		if err != nil {
			return nil, err
		}

		switch {
		case blockDesc.IsNormal():
			block, err := s.vbk.blockStore.Get(blockDesc.BlockID)
			if err != nil {
				return nil, err
			}

			buf, err := readAt(s.vbk.r, int64(block.Offset), int(block.CompressedSize))
			if err != nil {
				return nil, err
			}

			if block.IsCompressed() {
				switch block.CompressionType {
				case CompressionLZ4:
					if len(buf) < 12 {
						return nil, fmt.Errorf("%w: malformed lz4 block", ErrVBK)
					}
					dst := make([]byte, block.SourceSize)
					n, err := lz4.UncompressBlock(buf[12:], dst)
					if err != nil {
						return nil, err
					}
					buf = dst[:n]
				default:
					return nil, fmt.Errorf("%w: %d", ErrUnsupportedCompress, block.CompressionType)
				}
			}

			start := int(offsetInBlock)
			end := start + int(readSize)
			if end > len(buf) {
				return nil, fmt.Errorf("%w: block read out of bounds", ErrVBK)
			}
			result = append(result, buf[start:end]...)
		case blockDesc.IsSparse():
			result = append(result, make([]byte, readSize)...)
		default:
			return nil, fmt.Errorf("%w: %d", ErrUnsupportedBlock, blockDesc.Type)
		}

		offset += readSize
		length -= readSize
	}

	return result, nil
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}
