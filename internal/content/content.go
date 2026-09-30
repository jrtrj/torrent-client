// Package content decides how a torrent's contiguous byte stream maps onto
// files on disk, and hands back the store that speaks that mapping.
//
// Both the download path and the seed path go through here so they cannot
// disagree about where a piece lives. That matters because the two are usually
// different processes: a client that wrote out/<name>/a.bin and a seeder that
// looked for out/a.bin would serve nothing, and the bug would look like a
// networking failure.
package content

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"torrent-client/internal/metainfo"
	"torrent-client/internal/storage"
)

// OutputFor decides where a torrent's content belongs, given the path the user
// asked for. A multi-file torrent lives in a directory named after the torrent
// under that path; a single-file torrent is the file itself, or the file inside
// the path when the path names a directory.
func OutputFor(meta *metainfo.MetaInfo, output string) (string, error) {
	if meta.Multifile() {
		if err := storage.ValidateName(meta.Info.Name); err != nil {
			return "", fmt.Errorf("refusing torrent: name %q is not a safe directory name: %w", meta.Info.Name, err)
		}
		return filepath.Join(output, meta.Info.Name), nil
	}
	if strings.HasSuffix(output, string(os.PathSeparator)) {
		return filepath.Join(output, meta.Info.Name), nil
	}
	if info, err := os.Stat(output); err == nil && info.IsDir() {
		return filepath.Join(output, meta.Info.Name), nil
	}
	return output, nil
}

// Open builds the store for meta's content at root, creating whatever files or
// directories it needs. A multi-file torrent's paths are validated inside
// storage before anything is created, so a hostile .torrent cannot escape root.
func Open(meta *metainfo.MetaInfo, root string) (*storage.Storage, error) {
	if !meta.Multifile() {
		return storage.Open(root, meta.Info.PieceLength, meta.TotalLength())
	}
	files := make([]storage.FileSpec, 0, len(meta.Info.Files))
	for _, f := range meta.Info.Files {
		files = append(files, storage.FileSpec{Path: f.Path, Length: f.Length})
	}
	return storage.OpenMulti(root, files, meta.Info.PieceLength, meta.TotalLength())
}
