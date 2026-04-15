package vbk

import (
	"fmt"
)

type MetaBlob struct {
	slot *SnapshotSlot
	root int64
}

func (m *MetaBlob) read() ([]int64, [][]byte, error) {
	pages := make([]int64, 0, 16)
	buffers := make([][]byte, 0, 16)
	page := m.root

	for page != -1 {
		buf, err := m.slot.Page(page)
		if err != nil {
			return nil, nil, err
		}
		pages = append(pages, page)
		buffers = append(buffers, buf)
		page = leI64(buf, 0)
	}

	return pages, buffers, nil
}

func (m *MetaBlob) Pages() ([]int64, error) {
	pages, _, err := m.read()
	return pages, err
}

func (m *MetaBlob) Data() ([]byte, error) {
	_, buffers, err := m.read()
	if err != nil {
		return nil, err
	}
	out := make([]byte, 0, len(buffers)*PAGE_SIZE)
	for _, b := range buffers {
		out = append(out, b...)
	}
	return out, nil
}

type parseFunc[T any] func(*VBK, []byte) (T, error)

type metaVector[T any] struct {
	vbk            *VBK
	count          uint64
	entrySize      int
	entriesPerPage int
	parse          parseFunc[T]
	cache          map[uint64]T

	useVector2 bool
	pages      []int64
	table      []int64
	lookupMemo map[uint64]int64
}

func newMetaVector[T any](vbk *VBK, entrySize int, parser parseFunc[T], page int64, count uint64) (*metaVector[T], error) {
	v := &metaVector[T]{
		vbk:            vbk,
		count:          count,
		entrySize:      entrySize,
		entriesPerPage: PAGE_SIZE / entrySize,
		parse:          parser,
		cache:          make(map[uint64]T),
		lookupMemo:     make(map[uint64]int64),
	}

	if vbk.FormatVersion >= 12 && vbk.FormatVersion != 0x10008 {
		v.useVector2 = true
		blobData, err := vbk.MetaBlob(page).Data()
		if err != nil {
			return nil, err
		}
		v.table = make([]int64, 0, len(blobData)/8)
		for i := 0; i+8 <= len(blobData); i += 8 {
			v.table = append(v.table, leI64(blobData, i))
		}
		return v, nil
	}

	pages, err := vbk.MetaBlob(page).Pages()
	if err != nil {
		return nil, err
	}
	v.pages = pages
	return v, nil
}

func (v *metaVector[T]) Len() uint64 { return v.count }

func (v *metaVector[T]) Get(idx uint64) (T, error) {
	if idx >= v.count {
		var zero T
		return zero, ErrIndexOutOfRange
	}
	if item, ok := v.cache[idx]; ok {
		return item, nil
	}

	buf, err := v.data(idx)
	if err != nil {
		var zero T
		return zero, err
	}
	item, err := v.parse(v.vbk, buf)
	if err != nil {
		var zero T
		return zero, err
	}
	v.cache[idx] = item
	return item, nil
}

func (v *metaVector[T]) data(idx uint64) ([]byte, error) {
	if v.useVector2 {
		pageIdx := idx / uint64(v.entriesPerPage)
		offset := int((idx % uint64(v.entriesPerPage)) * uint64(v.entrySize))
		pageNo := v.lookupPage(pageIdx)
		buf, err := v.vbk.Page(pageNo)
		if err != nil {
			return nil, err
		}
		if offset+v.entrySize > len(buf) {
			return nil, fmt.Errorf("%w: metavector2 slice out of page bounds", ErrVBK)
		}
		return append([]byte(nil), buf[offset:offset+v.entrySize]...), nil
	}

	pageID := idx / uint64(v.entriesPerPage)
	offset := int((idx % uint64(v.entriesPerPage)) * uint64(v.entrySize))
	if pageID >= uint64(len(v.pages)) {
		return nil, fmt.Errorf("%w: metavector page index out of bounds", ErrVBK)
	}
	buf, err := v.vbk.Page(v.pages[pageID])
	if err != nil {
		return nil, err
	}
	offset += 8
	if offset+v.entrySize > len(buf) {
		return nil, fmt.Errorf("%w: metavector slice out of page bounds", ErrVBK)
	}
	return append([]byte(nil), buf[offset:offset+v.entrySize]...), nil
}

func (v *metaVector[T]) lookupPage(idx uint64) int64 {
	if cached, ok := v.lookupMemo[idx]; ok {
		return cached
	}

	const maxTableEntriesPerPage = PAGE_SIZE / 8
	maxLookup := [3]int{maxTableEntriesPerPage - 1, maxTableEntriesPerPage - 4, maxTableEntriesPerPage - 1}

	orig := idx
	if idx < uint64(maxTableEntriesPerPage-2) {
		page := v.table[idx+2]
		v.lookupMemo[orig] = page
		return page
	}

	idx -= uint64(maxTableEntriesPerPage - 2)
	tableIdx := 1
	for {
		maxEntries := maxLookup[tableIdx%3]
		if idx < uint64(maxEntries) {
			tableOffset := tableIdx * maxTableEntriesPerPage
			entryOffset := (maxTableEntriesPerPage - maxEntries) + int(idx)
			page := v.table[tableOffset+entryOffset]
			v.lookupMemo[orig] = page
			return page
		}
		idx -= uint64(maxEntries)
		tableIdx++
	}
}

type FibMetaSparseTable struct {
	vbk        *VBK
	page       int64
	count      uint64
	fakeSparse FibBlockDescriptor
	vec        *metaVector[MetaTableDescriptor]
	open       map[[2]uint64]*metaVector[FibBlockDescriptor]
}

const maxEntriesPerSparseTable = 1088

func newFibMetaSparseTable(vbk *VBK, page int64, count uint64) (*FibMetaSparseTable, error) {
	fake := FibBlockDescriptor{BlockSize: vbk.BlockSize, Type: BlockSparse}
	tableCount := (count + maxEntriesPerSparseTable - 1) / maxEntriesPerSparseTable

	vec, err := newMetaVector(vbk, metaTableDescriptorSize, parseMetaTableDescriptor, page, tableCount)
	if err != nil {
		return nil, err
	}

	return &FibMetaSparseTable{
		vbk:        vbk,
		page:       page,
		count:      count,
		fakeSparse: fake,
		vec:        vec,
		open:       make(map[[2]uint64]*metaVector[FibBlockDescriptor]),
	}, nil
}

func (t *FibMetaSparseTable) get(idx uint64) (FibBlockDescriptor, error) {
	if idx >= t.count {
		return FibBlockDescriptor{}, ErrIndexOutOfRange
	}

	tableIdx := idx / maxEntriesPerSparseTable
	entryIdx := idx % maxEntriesPerSparseTable

	tableEntry, err := t.vec.Get(tableIdx)
	if err != nil {
		return FibBlockDescriptor{}, err
	}
	if tableEntry.RootPage == -1 {
		return t.fakeSparse, nil
	}

	key := [2]uint64{uint64(tableEntry.RootPage), tableEntry.Count}
	open, ok := t.open[key]
	if !ok {
		parser := parseFibBlockDescriptor
		size := fibBlockDescriptorSize
		if t.vbk.IsV7() {
			parser = parseFibBlockDescriptorV7
			size = fibBlockDescriptorV7Size
		}
		open, err = newMetaVector(t.vbk, size, parser, tableEntry.RootPage, tableEntry.Count)
		if err != nil {
			return FibBlockDescriptor{}, err
		}
		t.open[key] = open
	}

	return open.Get(entryIdx)
}
