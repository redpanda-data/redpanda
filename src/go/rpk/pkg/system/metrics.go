// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"strconv"
	"time"

	"github.com/shirou/gopsutil/v3/disk"
	"github.com/shirou/gopsutil/v3/mem"
	"github.com/shirou/gopsutil/v3/process"
	"github.com/spf13/afero"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
)

type Metrics struct {
	CpuPercentage	float64
	FreeMemoryMB	float64
	FreeSpaceMB	float64
}

func GatherMetrics(
	fs afero.Fs, timeout time.Duration, conf config.Config,
) (*Metrics, []error) {
	metrics := &Metrics{}
	errs := []error{}
	cpuPercentage, err := redpandaCpuPercentage(fs, conf.PIDFile())
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.CpuPercentage = cpuPercentage
	}
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.FreeMemoryMB = float64(memInfo.Free) / 1024.0 / 1024.0
	}
	diskInfo, err := disk.Usage(conf.Redpanda.Directory)
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.FreeSpaceMB = float64(diskInfo.Free) / 1024.0 / 1024.0
	}

	return metrics, errs
}

func redpandaCpuPercentage(fs afero.Fs, pidFile string) (float64, error) {
	pidStr, err := utils.ReadEnsureSingleLine(fs, pidFile)
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		return 0, err
	}
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0, err
	}
	return p.Percent(1 * time.Second)
}
