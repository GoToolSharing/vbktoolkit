package vbk

import (
	"errors"
	"io"
	"testing"
)

func TestToExtPath(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{in: "/", want: "."},
		{in: "", want: "."},
		{in: "/etc/passwd", want: "etc/passwd"},
		{in: "etc/passwd", want: "etc/passwd"},
	}

	for _, tc := range tests {
		got := toExtPath(tc.in)
		if got != tc.want {
			t.Fatalf("toExtPath(%q)=%q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestReaderAtStorageReadAtBounds(t *testing.T) {
	s := &readerAtStorage{r: bytesReaderAt([]byte("abcdef")), size: 6}
	buf := make([]byte, 4)

	n, err := s.ReadAt(buf, 4)
	if n != 2 {
		t.Fatalf("unexpected n=%d", n)
	}
	if !errors.Is(err, io.EOF) {
		t.Fatalf("expected EOF, got %v", err)
	}
	if string(buf[:n]) != "ef" {
		t.Fatalf("unexpected data=%q", string(buf[:n]))
	}
}
