package sourceresolver

import (
	"archive/tar"
	"bytes"
	"compress/zlib"
	"context"
	"crypto/sha1"
	"encoding/binary"
	"errors"
	"io"
	"sort"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/project"
)

type fixtureFile struct {
	data string
	mode string
}

func snapshotObjects(files map[string]fixtureFile) (objectSet, plumbing.Hash) {
	objects := objectSet{}
	var tree func(string) plumbing.Hash
	tree = func(prefix string) plumbing.Hash {
		names := map[string]bool{}
		for filename := range files {
			if strings.HasPrefix(filename, prefix) {
				name := strings.SplitN(strings.TrimPrefix(filename, prefix), "/", 2)[0]
				names[name] = true
			}
		}
		ordered := []string{}
		for name := range names {
			ordered = append(ordered, name)
		}
		sort.Strings(ordered)
		var body bytes.Buffer
		for _, name := range ordered {
			mode := "40000"
			var hash plumbing.Hash
			if file, ok := files[prefix+name]; ok {
				mode = file.mode
				if mode == "" {
					mode = "100644"
				}
				blob := &gitObject{kind: plumbing.BlobObject, data: []byte(file.data)}
				blob.hash = plumbing.ComputeHash(blob.kind, blob.data)
				objects[blob.hash] = blob
				hash = blob.hash
			} else {
				hash = tree(prefix + name + "/")
			}
			body.WriteString(mode + " " + name)
			body.WriteByte(0)
			body.Write(hash[:])
		}
		object := &gitObject{kind: plumbing.TreeObject, data: body.Bytes()}
		object.hash = plumbing.ComputeHash(object.kind, object.data)
		objects[object.hash] = object
		return object.hash
	}
	root := tree("")
	commit := &gitObject{kind: plumbing.CommitObject, data: []byte("tree " + root.String() + "\nauthor Test <test@example.invalid> 0 +0000\ncommitter Test <test@example.invalid> 0 +0000\n\nlocal fixture\n")}
	commit.hash = plumbing.ComputeHash(commit.kind, commit.data)
	objects[commit.hash] = commit
	return objects, commit.hash
}

type packObject struct {
	kind     plumbing.ObjectType
	data     []byte
	base     plumbing.Hash
	declared int64
}

func packBytes(entries []packObject) []byte {
	var buf bytes.Buffer
	buf.WriteString("PACK")
	_ = binary.Write(&buf, binary.BigEndian, uint32(2))
	_ = binary.Write(&buf, binary.BigEndian, uint32(len(entries)))
	for _, entry := range entries {
		size := int64(len(entry.data))
		if entry.declared != 0 {
			size = entry.declared
		}
		first := byte(entry.kind)<<4 | byte(size&15)
		size >>= 4
		if size > 0 {
			first |= 128
		}
		buf.WriteByte(first)
		for size > 0 {
			next := byte(size & 127)
			size >>= 7
			if size > 0 {
				next |= 128
			}
			buf.WriteByte(next)
		}
		if entry.kind == plumbing.REFDeltaObject {
			buf.Write(entry.base[:])
		}
		writer := zlib.NewWriter(&buf)
		_, _ = writer.Write(entry.data)
		_ = writer.Close()
	}
	checksum := sha1.Sum(buf.Bytes())
	buf.Write(checksum[:])
	return buf.Bytes()
}
func packSnapshot(objects objectSet) []byte {
	hashes := []string{}
	for hash := range objects {
		hashes = append(hashes, hash.String())
	}
	sort.Strings(hashes)
	entries := []packObject{}
	for _, hash := range hashes {
		object := objects[plumbing.NewHash(hash)]
		entries = append(entries, packObject{kind: object.kind, data: object.data})
	}
	return packBytes(entries)
}
func deltaBytes(base, target []byte) []byte {
	var header [20]byte
	n := binary.PutUvarint(header[:], uint64(len(base)))
	m := binary.PutUvarint(header[n:], uint64(len(target)))
	out := append([]byte(nil), header[:n+m]...)
	out = append(out, byte(len(target)))
	return append(out, target...)
}
func TestBoundedPackResolvesForwardDeltasAndFullCommitTree(t *testing.T) {
	base, target := []byte("hello"), []byte("hello world")
	data := packBytes([]packObject{{kind: plumbing.REFDeltaObject, base: plumbing.ComputeHash(plumbing.BlobObject, base), data: deltaBytes(base, target)}, {kind: plumbing.BlobObject, data: base}})
	objects, err := decodePack(context.Background(), data, BoundedLimits())
	if err != nil || !bytes.Equal(objects[plumbing.ComputeHash(plumbing.BlobObject, target)].data, target) {
		t.Fatal("bounded forward delta resolution failed", err)
	}
	fixture, commit := snapshotObjects(map[string]fixtureFile{"Dockerfile": {data: "FROM scratch\n"}, "bin/start": {data: "#!/bin/sh\nexit 0\n", mode: "100755"}})
	decoded, err := decodePack(context.Background(), packSnapshot(fixture), BoundedLimits())
	if err != nil {
		t.Fatal(err)
	}
	selection := buildplan.SourceSelection{Binding: buildplan.SourceBinding{SelectionHash: strings.Repeat("a", 64)}}
	archive, watch, count, err := fullArchive(context.Background(), decoded, commit, selection, BoundedLimits())
	if err != nil || count != 2 || watch == "" {
		t.Fatal("full commit archive failed", err)
	}
	reader := tar.NewReader(bytes.NewReader(archive))
	for {
		header, err := reader.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatal(err)
		}
		if header.Name == "bin/start" && header.Mode != 0755 {
			t.Fatal("executable mode lost")
		}
	}
}
func TestPackBoundsApplyBeforeInflationAndDeltaTargetAllocation(t *testing.T) {
	for _, tc := range []struct {
		name   string
		data   []byte
		limits Limits
	}{
		{"declared huge object", packBytes([]packObject{{kind: plumbing.BlobObject, data: []byte("tiny"), declared: 1 << 40}}), BoundedLimits()},
		{"inflation mismatch", packBytes([]packObject{{kind: plumbing.BlobObject, data: bytes.Repeat([]byte("x"), 1000), declared: 1}}), BoundedLimits()},
		{"thin missing base", packBytes([]packObject{{kind: plumbing.REFDeltaObject, data: deltaBytes([]byte("a"), []byte("b"))}}), BoundedLimits()},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := decodePack(context.Background(), tc.data, tc.limits); err == nil {
				t.Fatal("adversarial pack accepted")
			}
		})
	}
	base := []byte("base")
	var targetHeader [20]byte
	n := binary.PutUvarint(targetHeader[:], uint64(len(base)))
	n += binary.PutUvarint(targetHeader[n:], 1<<40)
	data := packBytes([]packObject{{kind: plumbing.BlobObject, data: base}, {kind: plumbing.REFDeltaObject, base: plumbing.ComputeHash(plumbing.BlobObject, base), data: targetHeader[:n]}})
	if _, err := decodePack(context.Background(), data, BoundedLimits()); !errors.Is(err, errBudget) {
		t.Fatal("huge delta target was not rejected before patch")
	}
	limits := BoundedLimits()
	limits.InflatedBytes = 7
	if _, err := decodePack(context.Background(), packBytes([]packObject{{kind: plumbing.BlobObject, data: []byte("1234")}, {kind: plumbing.BlobObject, data: []byte("5678")}}), limits); !errors.Is(err, errBudget) {
		t.Fatal("aggregate inflated budget ignored")
	}
	valid := packBytes([]packObject{{kind: plumbing.BlobObject, data: base}})
	bad := append(append([]byte(nil), valid...), []byte("trailing")...)
	checksum := sha1.Sum(bad)
	bad = append(bad, checksum[:]...)
	if _, err := decodePack(context.Background(), bad, BoundedLimits()); err == nil {
		t.Fatal("trailing pack payload accepted")
	}
	valid[len(valid)-1] ^= 1
	if _, err := decodePack(context.Background(), valid, BoundedLimits()); err == nil {
		t.Fatal("bad pack checksum accepted")
	}
}
func TestDeltaDepthAndObjectCountBounds(t *testing.T) {
	entries := []packObject{{kind: plumbing.BlobObject, data: []byte("a")}}
	base := []byte("a")
	for _, target := range []string{"ab", "abc", "abcd"} {
		entries = append(entries, packObject{kind: plumbing.REFDeltaObject, base: plumbing.ComputeHash(plumbing.BlobObject, base), data: deltaBytes(base, []byte(target))})
		base = []byte(target)
	}
	limits := BoundedLimits()
	limits.DeltaDepth = 2
	if _, err := decodePack(context.Background(), packBytes(entries), limits); !errors.Is(err, errBudget) {
		t.Fatal("delta chain budget ignored")
	}
	limits = BoundedLimits()
	limits.Objects = 1
	if _, err := decodePack(context.Background(), packBytes(entries), limits); !errors.Is(err, errBudget) {
		t.Fatal("object count budget ignored")
	}
}
func TestFullArchiveDoesNotPruneWatchIgnoredBuildInputs(t *testing.T) {
	files := map[string]fixtureFile{"app/Dockerfile": {data: "FROM scratch\n"}, "app/main.go": {data: "package main\n"}, "docs/readme.md": {data: "old docs"}}
	selection := buildplan.SourceSelection{Source: project.Source{GitRepository: &project.SourceGitRepository{SparseCheckout: []string{"app"}}}, Binding: buildplan.SourceBinding{SelectionHash: strings.Repeat("a", 64)}}
	objects, commit := snapshotObjects(files)
	first, watch, count, err := fullArchive(context.Background(), objects, commit, selection, BoundedLimits())
	if err != nil || count != 3 {
		t.Fatal(err)
	}
	files["docs/readme.md"] = fixtureFile{data: "new docs"}
	objects, commit = snapshotObjects(files)
	second, nextWatch, _, err := fullArchive(context.Background(), objects, commit, selection, BoundedLimits())
	if err != nil || watch != nextWatch || bytes.Equal(first, second) {
		t.Fatal("watch filtering pruned build inputs or included ignored change")
	}
	again, againWatch, _, _ := fullArchive(context.Background(), objects, commit, selection, BoundedLimits())
	if !bytes.Equal(second, again) || nextWatch != againWatch {
		t.Fatal("archive/digest nondeterministic")
	}
}
func TestUnsupportedTreeAndWatchFeaturesFailExplicitly(t *testing.T) {
	for _, file := range []fixtureFile{{data: "target", mode: "120000"}, {data: "module", mode: "160000"}, {data: "version https://git-lfs.github.com/spec/v1\noid sha256:opaque\nsize 5\n"}} {
		objects, commit := snapshotObjects(map[string]fixtureFile{"unsupported": file})
		if _, _, _, err := fullArchive(context.Background(), objects, commit, buildplan.SourceSelection{}, BoundedLimits()); !errors.Is(err, errTree) {
			t.Fatal("unsupported source feature accepted")
		}
	}
	objects, commit := snapshotObjects(map[string]fixtureFile{".sourceignore": {data: "private-filter"}})
	if _, _, _, err := fullArchive(context.Background(), objects, commit, buildplan.SourceSelection{}, BoundedLimits()); !errors.Is(err, errWatch) {
		t.Fatal("nested sourceignore parity silently assumed")
	}
	for _, pattern := range []string{"[unmatched", "private\\ escaped", "a/**suffix", "../escape"} {
		selection := buildplan.SourceSelection{Source: project.Source{GitRepository: &project.SourceGitRepository{Ignore: &pattern}}}
		if _, err := watchDigest(nil, selection); !errors.Is(err, errWatch) {
			t.Fatal("unsupported filter syntax accepted")
		}
	}
	selection := buildplan.SourceSelection{Source: project.Source{GitRepository: &project.SourceGitRepository{SparseCheckout: []string{"app/../other"}}}}
	if _, err := watchDigest(nil, selection); !errors.Is(err, errWatch) {
		t.Fatal("sparse traversal normalized silently")
	}
}
