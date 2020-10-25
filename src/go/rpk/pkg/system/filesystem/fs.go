package filesystem

import (
	"path/filepath"
	"syscall"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
)

func DirectoryIsWriteable(fs afero.Fs, path string) (bool, error) {
	if exists, _ := afero.Exists(fs, path); !exists {
		err := fs.MkdirAll(path, 0755)
		if err != nil {
			return false, nil
		}
	}
	testFile := filepath.Join(path, "test_file")
	err := afero.WriteFile(fs, testFile, []byte{0}, 0644)
	if err != nil {
		return false, nil
	}
	err = fs.Remove(testFile)
	if err != nil {
		return false, err
	}

	return true, nil
}

func GetFreeDiskSpaceGB(path string) (float64, error) {
	statFs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &statFs)
	if err != nil {
		return 0, err
	}
	return float64(statFs.Bfree*uint64(statFs.Bsize)) / units.GiB, nil
}
