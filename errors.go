package vbk

import "errors"

var (
	ErrVBK                 = errors.New("vbk error")
	ErrNoActiveSlot        = errors.New("no active VBK metadata slot found")
	ErrFileNotFound        = errors.New("file not found")
	ErrNotDirectory        = errors.New("not a directory")
	ErrIsDirectory         = errors.New("not a file")
	ErrUnsupportedProperty = errors.New("unsupported property type")
	ErrUnsupportedBlock    = errors.New("unsupported block type")
	ErrUnsupportedCompress = errors.New("unsupported compression type")
	ErrIndexOutOfRange     = errors.New("index out of range")
)
