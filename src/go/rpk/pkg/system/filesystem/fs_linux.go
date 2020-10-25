package filesystem

import (
	"syscall"

	"golang.org/x/sys/unix"
)

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
