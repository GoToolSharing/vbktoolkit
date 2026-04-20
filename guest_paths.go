package vbk

import (
	"errors"
	"io"
	"path"
	"strings"
	"sync"
)

type lockedReadSeekerAt struct {
	r  io.ReadSeeker
	mu sync.Mutex
}

func (l *lockedReadSeekerAt) ReadAt(p []byte, off int64) (int, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if _, err := l.r.Seek(off, io.SeekStart); err != nil {
		return 0, err
	}

	read := 0
	for read < len(p) {
		n, err := l.r.Read(p[read:])
		read += n
		if err != nil {
			if errors.Is(err, io.EOF) {
				if read == len(p) {
					return read, nil
				}
				return read, io.EOF
			}
			return read, err
		}
		if n == 0 {
			break
		}
	}

	if read < len(p) {
		return read, io.EOF
	}
	return read, nil
}

func normalizeGuestPath(p, cwd string) string {
	p = strings.ReplaceAll(strings.TrimSpace(p), "\\", "/")
	if p == "" {
		if cwd == "" {
			return "/"
		}
		return cwd
	}

	if len(p) >= 2 && p[1] == ':' {
		p = p[2:]
	}
	if !strings.HasPrefix(p, "/") {
		p = joinGuestPath(cwd, p)
	}

	clean := path.Clean(p)
	if !strings.HasPrefix(clean, "/") {
		clean = "/" + clean
	}
	return clean
}

func joinGuestPath(base, name string) string {
	if base == "" || base == "/" {
		return "/" + strings.TrimLeft(name, "/")
	}
	return strings.TrimRight(base, "/") + "/" + strings.TrimLeft(name, "/")
}
