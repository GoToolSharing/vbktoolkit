# vbktoolkit

`vbktoolkit` is a Go library to read and extract content from Veeam `.vbk` backup files.

It is a port of the VBK logic from the Python `dissect.archive.vbk` implementation, adapted to idiomatic Go and split into maintainable packages/files.

## Features

- Parse VBK metadata (storage header, snapshot slots, banks, descriptors)
- Navigate VBK internal directory structure
- Read internal files as streams
- Handle sparse blocks
- Decompress LZ4-compressed blocks
- Support MetaVector and MetaVector2 layouts
- Parse property dictionaries
- Discover guest volumes embedded in VBK virtual disks (`.vhd`, `.vhdx`, `.vmdk`)
- Discover guest volumes from a standalone virtual disk on disk via `OpenDisk` (`.vhdx`, single-file `.vmdk`, raw/fixed-VHD), no VBK required
- Parse GPT partition layout from embedded virtual disks
- Read NTFS, EXT (ext2/3/4), and XFS guest files/directories (read-only)

## Installation

```bash
go get github.com/GoToolSharing/vbktoolkit
```

## Quick Start

```go
package main

import (
    "fmt"
    "io"
    "log"

    vbk "github.com/GoToolSharing/vbktoolkit"
)

func main() {
    backup, file, err := vbk.Open("/path/to/backup.vbk", true)
    if err != nil {
        log.Fatal(err)
    }
    defer file.Close()

    root, err := backup.Get("/", nil)
    if err != nil {
        log.Fatal(err)
    }

    entries, err := root.IterDir()
    if err != nil {
        log.Fatal(err)
    }

    for _, e := range entries {
        fmt.Printf("%s (dir=%v)\n", e.Name, e.IsDir())
    }

    item, err := backup.Get("/summary.xml", nil)
    if err != nil {
        log.Fatal(err)
    }

    stream, err := item.Open()
    if err != nil {
        log.Fatal(err)
    }
    defer stream.Close()

    data, err := io.ReadAll(stream)
    if err != nil {
        log.Fatal(err)
    }

    fmt.Printf("Read %d bytes\n", len(data))
}
```

## Core API

- `Open(path string, verify bool) (*VBK, *os.File, error)`
- `New(r io.ReaderAt, verify bool) (*VBK, error)`
- `(*VBK).Get(path string, base *DirItem) (*DirItem, error)`
- `(*DirItem).IterDir() ([]*DirItem, error)`
- `(*DirItem).Open() (*FibStream, error)`
- `(*DirItem).Properties() (PropertiesDictionary, error)`
- `(*VBK).DiscoverGuest() (*Guest, error)`
- `OpenDisk(diskPath string) (*Guest, error)` — discover guest volumes from a standalone virtual disk (no VBK)
- `(*Guest).Volumes() []*GuestVolume`
- `(*Guest).DefaultIndex() int`
- `(*GuestVolume).ListDir(path string) ([]GuestEntry, error)`
- `(*GuestVolume).ReadFile(path string, limit int64) ([]byte, error)`

## Guest Volume Example

```go
guest, err := backup.DiscoverGuest()
if err != nil {
    log.Fatal(err)
}
defer guest.Close()

for _, vol := range guest.Volumes() {
    fmt.Printf("[%d] disk=%s volume=%d name=%q size=%d\n", vol.Index, vol.DiskPath, vol.VolumeIndex, vol.Name, vol.Size)
}

idx := guest.DefaultIndex()
vol := guest.Volumes()[idx]
entries, err := vol.ListDir("/")
if err != nil {
    log.Fatal(err)
}
for _, e := range entries {
    fmt.Printf("%s dir=%v size=%d\n", e.Path, e.IsDir, e.Size)
}
```

### Standalone disk (no VBK)

When you have the guest disk directly (e.g. a `.vhdx` exported on its own),
`OpenDisk` returns the same `*Guest` without needing a `.vbk`:

```go
guest, err := vbk.OpenDisk("/path/to/disk.vhdx")
if err != nil {
    log.Fatal(err)
}
defer guest.Close()

vol := guest.Volumes()[guest.DefaultIndex()]
entries, err := vol.ListDir("/")
// ... identical to the VBK-backed guest above
```

## Validation

```bash
go test ./...
```

## Notes

- Current implementation focuses on reliable read/extract workflows.
- Some advanced VBK features (for example encryption-specific handling) are not implemented yet.
- Guest filesystem support currently targets NTFS, EXT (ext2/3/4), and XFS partitions from embedded VHD/VHDX/VMDK disks.

## Related Project

For a ready-to-use CLI on top of this library, see:

- `https://github.com/GoToolSharing/vbkview`
