package filesystem

import (
	"path/filepath"
	"syscall"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
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

func GetFilesystemType(path string) (FsType, error) {
	statFs := syscall.Statfs_t{}
	err := syscall.Statfs(path, &statFs)
	if err != nil {
		return "", err
	}
	switch statFs.Type {
	case unix.EXT4_SUPER_MAGIC:
		return Ext, nil
	case unix.XFS_SUPER_MAGIC:
		return Xfs, nil
	case unix.TMPFS_MAGIC:
		return Tmpfs, nil
	case 0x4244:
		return Hfs, nil
	default:
		return Unknown, nil
	}
}
