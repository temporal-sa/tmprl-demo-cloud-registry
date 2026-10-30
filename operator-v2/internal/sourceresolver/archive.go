package sourceresolver

import (
	"archive/tar"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"path"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/format/gitignore"
	"github.com/temporal-sa/tmprl-demo-cloud-registry/operator-v2/internal/buildplan"
)

var errTree = errors.New("invalid or unsupported source tree")
var errWatch = errors.New("unsupported source watch filter profile")

type sourceFile struct {
	name   string
	mode   int64
	object *gitObject
}

func fullArchive(ctx context.Context, objects objectSet, commit plumbing.Hash, selection buildplan.SourceSelection, limits Limits) ([]byte, string, int, error) {
	object := objects[commit]
	if object == nil || object.kind != plumbing.CommitObject {
		return nil, "", 0, errTree
	}
	first, _, found := bytes.Cut(object.data, []byte{'\n'})
	if !found || len(first) != 45 || !bytes.HasPrefix(first, []byte("tree ")) || !commitPattern.Match(first[5:]) {
		return nil, "", 0, errTree
	}
	files := []sourceFile{}
	var contentBytes int64
	entriesVisited := 0
	var walk func(plumbing.Hash, string, int) error
	walk = func(hash plumbing.Hash, prefix string, depth int) error {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if depth > limits.Depth {
			return errBudget
		}
		tree := objects[hash]
		if tree == nil || tree.kind != plumbing.TreeObject {
			return errTree
		}
		data := tree.data
		seen := map[string]bool{}
		for len(data) > 0 {
			entriesVisited++
			if entriesVisited > limits.Files {
				return errBudget
			}
			entry, rest, found := bytes.Cut(data, []byte{0})
			if !found || len(rest) < 20 {
				return errTree
			}
			modeText, name, found := strings.Cut(string(entry), " ")
			if !found || name == "" || len(name) > 255 || name == "." || name == ".." || strings.EqualFold(name, ".git") || strings.ContainsAny(name, "/\\") || !utf8.ValidString(name) || strings.IndexFunc(name, unicode.IsControl) >= 0 || seen[name] {
				return errTree
			}
			seen[name] = true
			var objectHash plumbing.Hash
			copy(objectHash[:], rest[:20])
			data = rest[20:]
			mode, err := strconv.ParseUint(modeText, 8, 32)
			if err != nil {
				return errTree
			}
			filename := path.Join(prefix, name)
			if len(filename) > 1024 {
				return errBudget
			}
			switch mode {
			case 040000:
				if err = walk(objectHash, filename, depth+1); err != nil {
					return err
				}
			case 0100644, 0100755:
				blob := objects[objectHash]
				if blob == nil || blob.kind != plumbing.BlobObject {
					return errTree
				}
				line, _, _ := bytes.Cut(blob.data, []byte{'\n'})
				if strings.TrimSuffix(string(line), "\r") == "version https://git-lfs.github.com/spec/v1" {
					return errTree
				}
				if name == ".sourceignore" {
					return errWatch
				}
				contentBytes += int64(len(blob.data))
				if len(files) >= limits.Files || contentBytes > limits.ArchiveBytes {
					return errBudget
				}
				permission := int64(0644)
				if mode == 0100755 {
					permission = 0755
				}
				files = append(files, sourceFile{name: filename, mode: permission, object: blob})
			default:
				return errTree // no symlink, submodule or special-file fallback
			}
		}
		return nil
	}
	if err := walk(plumbing.NewHash(string(first[5:])), "", 0); err != nil {
		return nil, "", 0, err
	}
	sort.Slice(files, func(i, j int) bool { return files[i].name < files[j].name })
	watch, err := watchDigest(files, selection)
	if err != nil {
		return nil, "", 0, err
	}
	var archive bytes.Buffer
	writer := tar.NewWriter(&boundedWriter{writer: &archive, remaining: limits.ArchiveBytes})
	for _, file := range files {
		if ctx.Err() != nil {
			return nil, "", 0, ctx.Err()
		}
		header := &tar.Header{Name: file.name, Mode: file.mode, Size: int64(len(file.object.data)), ModTime: time.Unix(0, 0).UTC(), Typeflag: tar.TypeReg, Format: tar.FormatPAX}
		if writer.WriteHeader(header) != nil {
			return nil, "", 0, errBudget
		}
		if _, err = writer.Write(file.object.data); err != nil {
			return nil, "", 0, errBudget
		}
	}
	if writer.Close() != nil {
		return nil, "", 0, errBudget
	}
	return archive.Bytes(), watch, len(files), nil
}
func watchDigest(files []sourceFile, selection buildplan.SourceSelection) (string, error) {
	sparse := []string{}
	patterns := []gitignore.Pattern{}
	if git := selection.Source.GitRepository; git != nil {
		if len(git.SparseCheckout) > 128 {
			return "", errWatch
		}
		for _, raw := range git.SparseCheckout {
			for _, segment := range strings.Split(raw, "/") {
				if segment == ".." {
					return "", errWatch
				}
			}
			value := strings.TrimSuffix(raw, "/")
			if value == "" || path.IsAbs(value) || strings.ContainsAny(value, "*?[]\\!") || strings.IndexFunc(value, unicode.IsControl) >= 0 {
				return "", errWatch
			}
			value = path.Clean(value)
			if value == ".." || strings.HasPrefix(value, "../") {
				return "", errWatch
			}
			sparse = append(sparse, value)
		}
		if git.Ignore != nil {
			if len(*git.Ignore) > 32*1024 {
				return "", errWatch
			}
			lines := strings.Split(*git.Ignore, "\n")
			if len(lines) > 128 {
				return "", errWatch
			}
			for _, raw := range lines {
				line := strings.TrimSuffix(raw, "\r")
				if line == "" || strings.HasPrefix(line, "#") {
					continue
				}
				if strings.TrimSpace(line) != line || strings.ContainsAny(line, "?[]\\") || strings.IndexFunc(line, unicode.IsControl) >= 0 {
					return "", errWatch
				}
				body := strings.TrimSuffix(strings.TrimPrefix(strings.TrimPrefix(line, "!"), "/"), "/")
				if body == "" {
					return "", errWatch
				}
				for _, segment := range strings.Split(body, "/") {
					if segment == "" || segment == "." || segment == ".." || strings.Contains(segment, "**") && segment != "**" {
						return "", errWatch
					}
				}
				patterns = append(patterns, gitignore.ParsePattern(line, nil))
			}
		}
	}
	matcher := gitignore.NewMatcher(patterns)
	type item struct {
		Name, Blob string
		Mode       int64
	}
	items := []item{}
	for _, file := range files {
		selected := len(sparse) == 0
		for _, prefix := range sparse {
			if prefix == "." || file.name == prefix || strings.HasPrefix(file.name, prefix+"/") {
				selected = true
			}
		}
		if selected && !matcher.Match(strings.Split(file.name, "/"), false) {
			items = append(items, item{file.name, file.object.hash.String(), file.mode})
		}
	}
	data, _ := json.Marshal(struct {
		Profile, SelectionHash string
		Items                  []item
	}{WatchProfile, selection.Binding.SelectionHash, items})
	sum := sha256.Sum256(data)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}
