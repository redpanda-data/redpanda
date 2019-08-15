package disk

/*
  #include <linux/types.h>
  #include <stdlib.h>
  #include <linux/kdev_t.h>

  int go_udev_major(dev_t d) {
    return MAJOR(d);
  }
  int go_udev_minor(dev_t d) {
    return MINOR(d);
  }
  dev_t go_udev_mkdev(int major, int minor) {
    return MKDEV(major, minor);
  }
*/
import "C"

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type BlockDevice interface {
	Syspath() string
	Devnode() string
	Parent() BlockDevice
}

type blockDevice struct {
	syspath string
	devnode string
	parent  BlockDevice
}

func NewDevice(major, minor int, fs afero.Fs) (BlockDevice, error) {
	dev := C.go_udev_mkdev((C.int)(major), (C.int)(minor))

	udevMajor := int(C.go_udev_major(dev))
	udevMinor := int(C.go_udev_minor(dev))
	log.Debugf("Creating block device from number {%d, %d}", udevMajor, udevMinor)
	syspath, err := readSyspath(udevMajor, udevMinor)
	if err != nil {
		return nil, err
	}
	return deviceFromSystemPath(syspath, fs)
}

func (d *blockDevice) Syspath() string {
	return d.syspath
}

func (d *blockDevice) Devnode() string {
	return d.devnode
}

func (d *blockDevice) Parent() BlockDevice {
	return d.parent
}

func deviceFromSystemPath(syspath string, fs afero.Fs) (BlockDevice, error) {
	log.Debugf("Reading block device details from '%s'", syspath)
	lines, err := utils.ReadFileLines(fs, filepath.Join(syspath, "uevent"))
	if err != nil {
		return nil, err
	}
	deviceAttrs, err := parseUeventFile(lines)
	if err != nil {
		return nil, err
	}

	var parentPath = filepath.Dir(syspath)
	var parent BlockDevice
	if utils.FileExists(fs, filepath.Join(parentPath, "uevent")) {

		parent, err = deviceFromSystemPath(parentPath, fs)
		if err != nil {
			return nil, err
		}
	}

	return &blockDevice{
		syspath: syspath,
		devnode: filepath.Join("/dev", deviceAttrs["DEVNAME"]),
		parent:  parent,
	}, nil
}

func readSyspath(major, minor int) (string, error) {
	path := fmt.Sprintf("/sys/dev/block/%d:%d", major, minor)
	linkpath, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join("/sys/dev/block", linkpath))
}

func parseUeventFile(lines []string) (map[string]string, error) {
	deviceAttrs := make(map[string]string)
	for _, line := range lines {
		parts := strings.Split(line, "=")
		if len(parts) != 2 {
			return nil, errors.New("Malformed uevent file content")
		}
		deviceAttrs[parts[0]] = parts[1]
	}
	return deviceAttrs, nil
}
