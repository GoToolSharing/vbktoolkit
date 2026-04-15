package vbk

import (
	"encoding/binary"
	"testing"
)

func TestMetaVector2Lookup(t *testing.T) {
	const numPages = 11
	entries := make([][]byte, 0, numPages)
	entry := int64(0)

	for i := 0; i < numPages; i++ {
		var header []byte
		switch {
		case i == 0:
			header = make([]byte, 16)
			binary.LittleEndian.PutUint64(header[0:8], uint64(i+1))
			binary.LittleEndian.PutUint64(header[8:16], 0)
		case i%3 == 1:
			header = make([]byte, 32)
			next := int64(-1)
			if i+1 < numPages {
				next = int64(i + 1)
			}
			third := int64(-1)
			if i+1 < numPages {
				third = int64(i + 1)
			}
			fourth := int64(-1)
			if i+2 < numPages {
				fourth = int64(i + 2)
			}
			binary.LittleEndian.PutUint64(header[0:8], uint64(next))
			binary.LittleEndian.PutUint64(header[8:16], uint64(i))
			binary.LittleEndian.PutUint64(header[16:24], uint64(third))
			binary.LittleEndian.PutUint64(header[24:32], uint64(fourth))
		default:
			header = make([]byte, 8)
			next := int64(-1)
			if i+1 < numPages {
				next = int64(i + 1)
			}
			binary.LittleEndian.PutUint64(header[0:8], uint64(next))
		}

		n := (PAGE_SIZE / 8) - (len(header) / 8)
		data := make([]byte, n*8)
		for j := 0; j < n; j++ {
			binary.LittleEndian.PutUint64(data[j*8:(j+1)*8], uint64(entry+int64(j)))
		}
		entries = append(entries, append(header, data...))
		entry += int64(n)
	}

	rawBlob := make([]byte, 0, numPages*PAGE_SIZE)
	for _, p := range entries {
		rawBlob = append(rawBlob, p...)
	}

	v := &metaVector[MetaTableDescriptor]{table: make([]int64, 0, len(rawBlob)/8), lookupMemo: map[uint64]int64{}}
	for i := 0; i+8 <= len(rawBlob); i += 8 {
		v.table = append(v.table, leI64(rawBlob, i))
	}

	for i := uint64(0); i < uint64(entry); i++ {
		got := v.lookupPage(i)
		if got != int64(i) {
			t.Fatalf("lookupPage(%d)=%d, want %d", i, got, i)
		}
	}
}
