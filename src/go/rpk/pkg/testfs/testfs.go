// Package testfs provides helpers to setup an afero.MemMapFs for usage in
// tests.
package testfs

import (
	"io/fs"
	"path/filepath"
	"strings"

	"github.com/spf13/afero"
)

// Fmode tracks the mode of a file and its contents.
type Fmode struct {
	Mode     fs.FileMode
	Contents string
}

// FromMap returns an afero.MemMapFs from the input paths and their
// corresponding file contents & modes. All directories are created with perms
// 0644.
//
// Paths that end in / are treated solely as directories and the Fmode is not
// used. Similar to Go's os/fs, paths must use /, not os.PathSeparator.
func FromMap(m map[string]Fmode) afero.Fs {
	mmfs := afero.NewMemMapFs()
	for path, fmode := range m {
		dir := ""
		if strings.IndexByte(path, '/') != -1 {
			if path[len(path)-1] == '/' {
				mmfs.MkdirAll(path, 0644)
				continue
			}
			dir = filepath.Dir(path)
		}
		mmfs.MkdirAll(dir, 0644)
		afero.WriteFile(mmfs, path, []byte(fmode.Contents), fmode.Mode)
	}
	return mmfs
}
