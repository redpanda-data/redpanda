// Package testfs provides helpers to setup an afero.MemMapFs for usage in
// tests.
package testfs

import (
	"errors"
	"io/fs"
	"path/filepath"
	"strings"
	"testing"

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
				mmfs.MkdirAll(path, 0o755)
				continue
			}
			dir = filepath.Dir(path)
		}
		mmfs.MkdirAll(dir, 0o755)
		afero.WriteFile(mmfs, path, []byte(fmode.Contents), fmode.Mode)
	}
	return mmfs
}

// Expect ensures that all files in m exist in fs with the expected file mode
// and contents.
func Expect(t *testing.T, fs afero.Fs, m map[string]Fmode) {
	for path, fmode := range m {
		stat, err := fs.Stat(path)
		if err != nil {
			t.Errorf("stat %q failure: %v", path, err)
			continue
		}

		if mode := stat.Mode(); mode != fmode.Mode {
			t.Errorf("stat %q mode %v != expected %v", path, mode, fmode.Mode)
		}

		file, err := afero.ReadFile(fs, path)
		if err != nil {
			t.Errorf("unable to read %q: %v", path, err)
			continue
		}

		if got := string(file); got != fmode.Contents {
			t.Errorf("file %q contents %q != expected %q", path, got, fmode.Contents)
		}
	}
}

// ExpectNot ensures that the files in m do not exist in fs.
func ExpectNot(t *testing.T, afs afero.Fs, paths ...string) {
	for _, path := range paths {
		_, err := afs.Stat(path)
		switch {
		case errors.Is(err, fs.ErrNotExist):
		case err == nil:
			t.Errorf("stat %q shows file exists", path)
		default:
			t.Errorf("unable to stat %q: %v", path, err)
		}
	}
}
