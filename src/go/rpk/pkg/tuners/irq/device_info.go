// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package irq

import (
	"path"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type DeviceInfo interface {
	GetIRQs(irqConfigDir string, xenDeviceName string) ([]int, error)
}

func NewDeviceInfo(fs afero.Fs, procFile ProcFile) DeviceInfo {
	return &deviceInfo{
		procFile: procFile,
		fs:       fs,
	}
}

type deviceInfo struct {
	procFile ProcFile
	fs       afero.Fs
}

func (deviceInfo *deviceInfo) GetIRQs(
	irqConfigDir string, xenDeviceName string,
) ([]int, error) {
	log.Debugf("Reading IRQs of '%s', with deviceInfo name pattern '%s'", irqConfigDir, xenDeviceName)
	msiIRQsDirName := path.Join(irqConfigDir, "msi_irqs")
	var irqs []int
	if exists, _ := afero.Exists(deviceInfo.fs, msiIRQsDirName); exists {
		log.Debugf("Device '%s' uses MSI IRQs", irqConfigDir)
		files := utils.ListFilesInPath(deviceInfo.fs, msiIRQsDirName)
		for _, file := range files {
			irq, err := strconv.Atoi(file)
			if err != nil {
				return nil, err
			}
			irqs = append(irqs, irq)
		}
	} else {
		irqFileName := path.Join(irqConfigDir, "irq")
		if exists, _ := afero.Exists(deviceInfo.fs, irqFileName); exists {
			log.Debugf("Device '%s' uses INT#x IRQs", irqConfigDir)
			lines, err := utils.ReadFileLines(deviceInfo.fs, irqFileName)
			if err != nil {
				return nil, err
			}
			for _, rawLine := range lines {
				irq, err := strconv.Atoi(strings.TrimSpace(rawLine))
				if err != nil {
					return nil, err
				}
				irqs = append(irqs, irq)
			}
		} else {
			modAliasFileName := path.Join(irqConfigDir, "modalias")
			lines, err := utils.ReadFileLines(deviceInfo.fs, modAliasFileName)
			if err != nil {
				return nil, err
			}
			modAlias := lines[0]
			irqProcFileLines, err := deviceInfo.procFile.GetIRQProcFileLinesMap()
			if err != nil {
				return nil, err
			}
			if strings.Contains(modAlias, "virtio") {
				log.Debugf("Device '%s' is a virtio device type", irqConfigDir)
				fileNames := utils.ListFilesInPath(deviceInfo.fs, path.Join(irqConfigDir, "driver"))
				for _, name := range fileNames {
					if strings.Contains(name, "virtio") {
						irqs = append(irqs,
							deviceInfo.getIRQsForLinesMatching(name, irqProcFileLines)...)
					}
				}
			} else {
				if strings.Contains(modAlias, "xen:") {
					log.Debugf("Reading '%s' device IRQs from /proc/interrupts", irqConfigDir)
					irqs = deviceInfo.getIRQsForLinesMatching(xenDeviceName, irqProcFileLines)
				}
			}
		}
	}
	log.Debugf("DeviceInfo '%s' IRQs '%v'", irqConfigDir, irqs)
	return irqs, nil
}

func (deviceInfo *deviceInfo) getIRQsForLinesMatching(
	pattern string, irqToProcLineMap map[int]string,
) []int {
	var irqs []int
	for irq, line := range irqToProcLineMap {
		if strings.Contains(line, pattern) {
			irqs = append(irqs, irq)
		}
	}
	return irqs
}
