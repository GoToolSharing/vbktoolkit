package vbk

import (
	"encoding/binary"
	"fmt"
	"strings"
	"unicode/utf16"
)

type PropertiesDictionary map[string]any

func parseProperties(vbk *VBK, page int64) (PropertiesDictionary, error) {
	blob, err := vbk.MetaBlob(page).Data()
	if err != nil {
		return nil, err
	}
	if len(blob) < metaBlobHeaderSize {
		return nil, fmt.Errorf("%w: invalid properties blob", ErrVBK)
	}

	props := make(PropertiesDictionary)
	pos := metaBlobHeaderSize

	for {
		if pos+4 > len(blob) {
			return nil, fmt.Errorf("%w: malformed properties dictionary", ErrVBK)
		}
		vtype := PropertyType(leI32(blob, pos))
		pos += 4

		if vtype == PropertyEnd {
			break
		}
		if pos+4 > len(blob) {
			return nil, fmt.Errorf("%w: malformed property name length", ErrVBK)
		}
		nameLen := int(leU32(blob, pos))
		pos += 4
		if nameLen < 0 || pos+nameLen > len(blob) {
			return nil, fmt.Errorf("%w: malformed property name", ErrVBK)
		}
		name := string(blob[pos : pos+nameLen])
		pos += nameLen

		value, read, err := parsePropertyValue(vtype, blob[pos:])
		if err != nil {
			return nil, err
		}
		pos += read
		props[name] = value
	}

	return props, nil
}

func parsePropertyValue(vtype PropertyType, buf []byte) (any, int, error) {
	switch vtype {
	case PropertyUInt32:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("%w: malformed uint32 property", ErrVBK)
		}
		return leU32(buf, 0), 4, nil
	case PropertyUInt64:
		if len(buf) < 8 {
			return nil, 0, fmt.Errorf("%w: malformed uint64 property", ErrVBK)
		}
		return leU64(buf, 0), 8, nil
	case PropertyAString:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("%w: malformed string property", ErrVBK)
		}
		l := int(leU32(buf, 0))
		if len(buf) < 4+l {
			return nil, 0, fmt.Errorf("%w: malformed string property data", ErrVBK)
		}
		return string(buf[4 : 4+l]), 4 + l, nil
	case PropertyWString:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("%w: malformed wstring property", ErrVBK)
		}
		l := int(leU32(buf, 0))
		if len(buf) < 4+l {
			return nil, 0, fmt.Errorf("%w: malformed wstring property data", ErrVBK)
		}
		raw := buf[4 : 4+l]
		u16 := make([]uint16, 0, len(raw)/2)
		for i := 0; i+1 < len(raw); i += 2 {
			u16 = append(u16, binary.LittleEndian.Uint16(raw[i:i+2]))
		}
		return string(utf16.Decode(u16)), 4 + l, nil
	case PropertyBinary:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("%w: malformed binary property", ErrVBK)
		}
		l := int(leU32(buf, 0))
		if len(buf) < 4+l {
			return nil, 0, fmt.Errorf("%w: malformed binary property data", ErrVBK)
		}
		out := make([]byte, l)
		copy(out, buf[4:4+l])
		return out, 4 + l, nil
	case PropertyBoolean:
		if len(buf) < 4 {
			return nil, 0, fmt.Errorf("%w: malformed bool property", ErrVBK)
		}
		return leU32(buf, 0) != 0, 4, nil
	default:
		return nil, 0, fmt.Errorf("%w: %d", ErrUnsupportedProperty, vtype)
	}
}

type DirItem struct {
	vbk    *VBK
	rec    DirItemRecord
	Name   string
	root   int64
	count  uint64
	isRoot bool
}

func newRootDirItem(vbk *VBK, page int64, count uint64) *DirItem {
	return &DirItem{vbk: vbk, Name: "/", root: page, count: count, isRoot: true}
}

func parseDirItem(vbk *VBK, buf []byte) (*DirItem, error) {
	rec, err := parseDirItemRecord(buf)
	if err != nil {
		return nil, err
	}
	n := int(rec.NameLength)
	if n < 0 {
		n = 0
	}
	if n > len(rec.Name) {
		n = len(rec.Name)
	}
	name := string(rec.Name[:n])
	item := &DirItem{vbk: vbk, rec: rec, Name: name}

	if rec.Type == DirItemSubFolder {
		item.root = rec.subFolderRoot()
		item.count = rec.subFolderCount()
	}
	return item, nil
}

func (d *DirItem) Type() DirItemType {
	if d.isRoot {
		return DirItemSubFolder
	}
	return d.rec.Type
}

func (d *DirItem) IsDir() bool {
	return d.isRoot || d.rec.Type == DirItemSubFolder
}

func (d *DirItem) IsFile() bool {
	return d.IsInternalFile() || d.IsExternalFile()
}

func (d *DirItem) IsInternalFile() bool {
	return !d.isRoot && d.rec.Type == DirItemIntFib
}

func (d *DirItem) IsExternalFile() bool {
	return !d.isRoot && d.rec.Type == DirItemExtFib
}

func (d *DirItem) Size() (uint64, error) {
	switch d.rec.Type {
	case DirItemExtFib, DirItemIntFib, DirItemPatch, DirItemIncrement:
		return d.rec.fibSize(), nil
	default:
		return 0, fmt.Errorf("%w: size not available for %s", ErrVBK, d.Name)
	}
}

func (d *DirItem) Properties() (PropertiesDictionary, error) {
	if d.isRoot || d.rec.PropsRootPage == -1 {
		return nil, nil
	}
	return parseProperties(d.vbk, d.rec.PropsRootPage)
}

func (d *DirItem) IterDir() ([]*DirItem, error) {
	if !d.IsDir() {
		return nil, fmt.Errorf("%w: %s", ErrNotDirectory, d.Name)
	}

	vec, err := newMetaVector(d.vbk, dirItemRecordSize, func(v *VBK, buf []byte) (*DirItem, error) {
		return parseDirItem(v, buf)
	}, d.root, d.count)
	if err != nil {
		return nil, err
	}

	items := make([]*DirItem, 0, d.count)
	for i := uint64(0); i < d.count; i++ {
		entry, err := vec.Get(i)
		if err != nil {
			return nil, err
		}
		items = append(items, entry)
	}
	return items, nil
}

func (d *DirItem) ListDir() (map[string]*DirItem, error) {
	items, err := d.IterDir()
	if err != nil {
		return nil, err
	}
	out := make(map[string]*DirItem, len(items))
	for _, item := range items {
		out[item.Name] = item
	}
	return out, nil
}

func (d *DirItem) Open() (*FibStream, error) {
	if !d.IsInternalFile() {
		return nil, fmt.Errorf("%w: %s", ErrIsDirectory, d.Name)
	}
	size, _ := d.Size()
	return newFibStream(d.vbk, d.rec.blocksVectorRoot(), d.rec.blocksVectorCount(), int64(size))
}

func (d *DirItem) String() string {
	name := d.Name
	if strings.TrimSpace(name) == "" {
		name = "/"
	}
	return name
}
