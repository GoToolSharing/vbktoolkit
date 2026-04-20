package vbk

import (
	"errors"
	"io"
	"testing"
)

func TestExtPathComponents(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{in: "/", want: nil},
		{in: "", want: nil},
		{in: "/etc/passwd", want: []string{"etc", "passwd"}},
		{in: "etc/passwd", want: []string{"etc", "passwd"}},
	}

	for _, tc := range tests {
		got := extPathComponents(tc.in)
		if len(got) != len(tc.want) {
			t.Fatalf("extPathComponents(%q) len=%d, want %d", tc.in, len(got), len(tc.want))
		}
		for i := range got {
			if got[i] != tc.want[i] {
				t.Fatalf("extPathComponents(%q)[%d]=%q, want %q", tc.in, i, got[i], tc.want[i])
			}
		}
	}
}

func TestReadAllFromReaderAtBounds(t *testing.T) {
	r := bytesReaderAt([]byte("abcdef"))
	buf := make([]byte, 4)

	n, err := r.ReadAt(buf, 4)
	if n != 2 {
		t.Fatalf("unexpected n=%d", n)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	if string(buf[:n]) != "ef" {
		t.Fatalf("unexpected data=%q", string(buf[:n]))
	}

	all, err := readAllFromReaderAt(r, 6)
	if err != nil {
		t.Fatalf("readAllFromReaderAt error: %v", err)
	}
	if string(all) != "abcdef" {
		t.Fatalf("unexpected readAll data=%q", string(all))
	}
}
