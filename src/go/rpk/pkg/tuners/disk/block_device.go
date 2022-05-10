// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package disk

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
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

	parentPath := filepath.Dir(syspath)
	var parent BlockDevice
	if exists, _ := afero.Exists(fs, filepath.Join(parentPath, "uevent")); exists {
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

func readSyspath(major, minor uint32) (string, error) {
	blockBasePath := "/sys/dev/block"
	path := fmt.Sprintf("%s/%d:%d", blockBasePath, major, minor)
	linkpath, err := os.Readlink(path)
	if err != nil {
		return "", err
	}
	return filepath.Abs(filepath.Join(blockBasePath, linkpath))
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
