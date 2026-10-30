package sourceresolver

import (
	"bytes"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/packfile"
)

var errPack = errors.New("invalid or unsupported bounded source pack")
var errBudget = errors.New("source acquisition budget exceeded")

type gitObject struct {
	kind plumbing.ObjectType
	data []byte
	hash plumbing.Hash
}
type packEntry struct {
	header *packfile.ObjectHeader
	raw    []byte
	object *gitObject
	depth  int
}
type objectSet map[plumbing.Hash]*gitObject

// decodePack checks allocation budgets BEFORE inflation and delta application.
// Library Parser observers are deliberately unused: they run after allocation.
func decodePack(ctx context.Context, data []byte, limits Limits) (objectSet, error) {
	if !limits.valid() || len(data) < 32 || int64(len(data)) > limits.PackBytes {
		return nil, errBudget
	}
	expected := data[len(data)-20:]
	sum := sha1.Sum(data[:len(data)-20])
	if !bytes.Equal(sum[:], expected) {
		return nil, errPack
	}
	scanner := packfile.NewScanner(bytes.NewReader(data))
	_, count, err := scanner.Header()
	if err != nil || count == 0 || int64(count) > int64(limits.Objects) {
		return nil, errBudget
	}
	entries := make(map[int64]*packEntry, int(count))
	byHash := map[plumbing.Hash][]*packEntry{}
	byOffset := map[int64][]*packEntry{}
	queue := []*packEntry{}
	objects := objectSet{}
	remaining := limits.InflatedBytes
	for i := uint32(0); i < count; i++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		header, err := scanner.NextObjectHeader()
		if err != nil {
			return nil, errPack
		}
		if header.Length < 0 || header.Length > limits.ObjectBytes || header.Length > remaining {
			return nil, errBudget
		}
		remaining -= header.Length
		switch header.Type {
		case plumbing.CommitObject, plumbing.TreeObject, plumbing.BlobObject, plumbing.TagObject, plumbing.REFDeltaObject, plumbing.OFSDeltaObject:
		default:
			return nil, errPack
		}
		buf := bytes.NewBuffer(make([]byte, 0, int(header.Length)))
		writer := &boundedWriter{writer: buf, remaining: header.Length}
		n, _, err := scanner.NextObject(writer)
		if err != nil || n != header.Length || int64(buf.Len()) != header.Length {
			return nil, errPack
		}
		entry := &packEntry{header: header, raw: buf.Bytes()}
		entries[header.Offset] = entry
		switch header.Type {
		case plumbing.REFDeltaObject:
			byHash[header.Reference] = append(byHash[header.Reference], entry)
		case plumbing.OFSDeltaObject:
			if entries[header.OffsetReference] == nil {
				return nil, errPack
			}
			byOffset[header.OffsetReference] = append(byOffset[header.OffsetReference], entry)
		default:
			entry.object = &gitObject{kind: header.Type, data: entry.raw, hash: plumbing.ComputeHash(header.Type, entry.raw)}
			if err = objects.add(entry.object); err != nil {
				return nil, err
			}
			queue = append(queue, entry)
		}
	}
	footer, err := scanner.Checksum()
	if err != nil || !bytes.Equal(footer[:], expected) {
		return nil, errPack
	}
	// Resolve each node once; do not repeatedly scan unresolved REF deltas or
	// recursively inflate attacker-controlled chains. Missing/thin bases fail.
	resolved := 0
	for index := 0; index < len(queue); index++ {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		base := queue[index]
		resolved++
		children := append(byOffset[base.header.Offset], byHash[base.object.hash]...)
		for _, child := range children {
			if child.object != nil {
				continue
			}
			child.depth = base.depth + 1
			if child.depth > limits.DeltaDepth {
				return nil, errBudget
			}
			sourceSize, n := binary.Uvarint(child.raw)
			if n <= 0 || sourceSize != uint64(len(base.object.data)) {
				return nil, errPack
			}
			targetSize, m := binary.Uvarint(child.raw[n:])
			if m <= 0 {
				return nil, errPack
			}
			if targetSize > uint64(limits.ObjectBytes) || targetSize > uint64(remaining) {
				return nil, errBudget
			}
			remaining -= int64(targetSize)
			patched, err := packfile.PatchDelta(base.object.data, child.raw)
			if err != nil || uint64(len(patched)) != targetSize {
				return nil, errPack
			}
			child.object = &gitObject{kind: base.object.kind, data: patched, hash: plumbing.ComputeHash(base.object.kind, patched)}
			child.raw = nil
			if err = objects.add(child.object); err != nil {
				return nil, err
			}
			queue = append(queue, child)
		}
	}
	if resolved != int(count) {
		return nil, errPack
	}
	return objects, nil
}
func (s objectSet) add(o *gitObject) error {
	if old := s[o.hash]; old != nil && (old.kind != o.kind || !bytes.Equal(old.data, o.data)) {
		return errPack
	}
	s[o.hash] = o
	return nil
}

type boundedWriter struct {
	writer    io.Writer
	remaining int64
}

func (w *boundedWriter) Write(p []byte) (int, error) {
	if int64(len(p)) > w.remaining {
		return 0, errBudget
	}
	n, err := w.writer.Write(p)
	w.remaining -= int64(n)
	return n, err
}
