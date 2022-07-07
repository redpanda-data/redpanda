// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package system

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/tklauser/go-sysconf"
)

type Metrics struct {
	CPUPercentage float64 `json:"cpuPercentage"`
	FreeMemoryMB  float64 `json:"freeMemoryMB"`
	FreeSpaceMB   float64 `json:"freeSpaceMB"`
}

type stat struct {
	// Reference: https://man7.org/linux/man-pages/man5/proc.5.html

	// Amount of time that this process has been scheduled
	// in user mode, measured in clock ticks (divide by
	// sysconf(_SC_CLK_TCK)).  This includes guest_time (time
	// spent running a virtual CPU).
	utime uint64

	// Amount of time that this process has been scheduled
	// in kernel mode, measured in clock ticks (divide by
	// sysconf(_SC_CLK_TCK)).
	stime uint64
}

var errRedpandaDown = errors.New("the local redpanda process isn't running")

func GatherMetrics(
	fs afero.Fs, timeout time.Duration, conf config.Config,
) (*Metrics, error) {
	var err, errs error
	metrics := &Metrics{}

	metrics.FreeSpaceMB, err = getFreeDiskSpaceMB(conf)
	if err != nil {
		errs = multierror.Append(errs, err)
	}

	pidStr, err := utils.ReadEnsureSingleLine(fs, conf.PIDFile())
	if err != nil {
		if os.IsNotExist(err) {
			return metrics, errRedpandaDown
		}
		errs = multierror.Append(errs, err)
		return metrics, errs
	}
	pid, err := strconv.Atoi(pidStr)
	if err != nil {
		errs = multierror.Append(errs, err)
		return metrics, errs
	}

	metrics.CPUPercentage, err = redpandaCPUPercentage(fs, pid, timeout)
	if err != nil {
		errs = multierror.Append(errs, err)
	}

	memInfo, err := getMemInfo(fs)
	if err != nil {
		errs = multierror.Append(errs, err)
	} else {
		metrics.FreeMemoryMB = float64(memInfo.MemFree) / 1024.0 / 1024.0
	}

	return metrics, errs
}

func redpandaCPUPercentage(
	fs afero.Fs, pid int, timeout time.Duration,
) (float64, error) {
	clktck, err := sysconf.Sysconf(sysconf.SC_CLK_TCK)
	if err != nil {
		return 0, err
	}

	stat1, err := readStat(fs, pid)
	if err != nil {
		return 0, err
	}
	start := time.Now()

	time.Sleep(timeout)

	stat2, err := readStat(fs, pid)
	if err != nil {
		return 0, err
	}
	end := time.Now()

	cpuTime1 := stat1.utime + stat1.stime
	cpuTime2 := stat2.utime + stat2.stime

	// stime & utime are measured in jiffies, so we gotta divide by
	// SC_CLK_TCK (seconds per jiffy) to get the time in seconds.
	deltaCPUTime := float64(cpuTime2-cpuTime1) / float64(clktck)

	return (deltaCPUTime * 100.0) / end.Sub(start).Seconds(), nil
}

func readStat(fs afero.Fs, pid int) (*stat, error) {
	statFilePath := fmt.Sprintf("/proc/%d/stat", pid)
	line, err := utils.ReadEnsureSingleLine(fs, statFilePath)
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(line)

	// Reference: https://man7.org/linux/man-pages/man5/proc.5.html
	utime, err := strconv.ParseUint(fields[13], 10, 64)
	if err != nil {
		return nil, err
	}

	stime, err := strconv.ParseUint(fields[14], 10, 64)
	if err != nil {
		return nil, err
	}

	return &stat{
		utime: utime,
		stime: stime,
	}, nil
}

func getFreeDiskSpaceMB(conf config.Config) (float64, error) {
	var stat syscall.Statfs_t
	err := syscall.Statfs(conf.Redpanda.Directory, &stat)
	if err != nil {
		return 0, err
	}
	// Available blocks * block size (in bytes)
	freeSpaceBytes := stat.Bavail * uint64(stat.Bsize)
	return float64(freeSpaceBytes) / 1024.0 / 1024.0, nil
}

func IsErrRedpandaDown(err error) bool {
	return errors.Is(err, errRedpandaDown)
}
