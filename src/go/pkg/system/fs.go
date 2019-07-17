package system

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"vectorized/utils"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
)

type FsInfo struct {
	DeviceID       string
	MountPoint     string
	FilesystemType string
}

func DirectoryIsWriteable(fs afero.Fs, path string) (bool, error) {
	if !utils.FileExists(fs, path) {
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

func GetFilesystemType(fs afero.Fs, path string) (string, error) {
	mountPoints := make(map[string]string)
	file, err := fs.Open("/etc/fstab")
	if err != nil {
		return "", err
	}
	fsInfos, err := parseFsTab(file)
	if err != nil {
		return "", err
	}
	for _, fsInfo := range fsInfos {
		mountPoints[fsInfo.MountPoint] = fsInfo.FilesystemType
	}

	for dir := path; dir != string(filepath.Separator); dir = filepath.Dir(dir) {
		if filesytemType, exists := mountPoints[dir]; exists {
			return filesytemType, nil
		}
	}
	if filesytemType, exists := mountPoints["/"]; exists {
		return filesytemType, nil
	}

	return "", fmt.Errorf("Unable to find filesystem for path '%s'", path)
}

func parseFsTab(reader io.Reader) ([]FsInfo, error) {
	commentPattern := regexp.MustCompile(" *#.*")
	emptyLinePattern := regexp.MustCompile("^ *$")
	scanner := bufio.NewScanner(reader)
	var fsInfo []FsInfo
	for scanner.Scan() {
		line := scanner.Text()
		if commentPattern.MatchString(line) ||
			emptyLinePattern.MatchString(line) {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 {
			return nil, errors.New("Error parsing filesytem table")
		}
		fsInfo = append(fsInfo, FsInfo{
			DeviceID:       fields[0],
			MountPoint:     fields[1],
			FilesystemType: fields[2],
		})
	}
	return fsInfo, nil
}
