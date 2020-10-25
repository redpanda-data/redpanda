package filesystem

import "errors"

func GetFilesystemType(path string) (FsType, error) {
	return Unknown, errors.New("Filesystem detection not available for MacOS")
}
