// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package system

import (
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
)

const cgroupBaseDir = "/sys/fs/cgroup"

func ReadCgroupMemLimitBytes(fs afero.Fs) (uint64, error) {
	return readUintCgroupsProp(
		fs,
		"/memory/memory.limit_in_bytes",
		"/memory.max",
	)
}

func ReadCgroupEffectiveCpusNo(fs afero.Fs) (uint64, error) {
	cpuList, err := readCgroupFile(
		fs,
		"/cpuset/cpuset.effective_cpus",
		"/cpuset.cpus.effective",
	)
	if err != nil {
		return 0, err
	}
	return calculateEffectiveCpus(cpuList)
}

func readUintCgroupsProp(
	fs afero.Fs, v1Subpath, v2Subpath string,
) (uint64, error) {
	val, err := readCgroupFile(fs, v1Subpath, v2Subpath)
	if err != nil {
		return 0, err
	}
	if val == "max" {
		return math.MaxUint64, nil
	}
	return strconv.ParseUint(strings.TrimSpace(val), 10, 64)
}

func calculateEffectiveCpus(cpuList string) (uint64, error) {
	if cpuList == "" {
		return 0, errors.New("no CPUs assigned to process")
	}
	cpus := 0
	ranges := strings.Split(cpuList, ",")
	for _, r := range ranges {
		if r == "" {
			return 0, fmt.Errorf("missing value in cpu list '%s'", cpuList)
		}
		if !strings.Contains(r, "-") {
			cpus++
			continue
		}

		limits := strings.Split(r, "-")
		if len(limits) != 2 {
			return 0, fmt.Errorf(
				"invalid effective CPU range '%s' in '%s'",
				r,
				cpuList,
			)
		}
		lower, err := strconv.Atoi(limits[0])
		if err != nil {
			return 0, fmt.Errorf(
				"couldn't parse effective CPU lower range limit '%s' in '%s'",
				limits[0],
				cpuList,
			)
		}
		upper, err := strconv.Atoi(limits[1])
		if err != nil {
			return 0, fmt.Errorf(
				"couldn't parse effective CPU upper range limit '%s' in '%s'",
				limits[1],
				cpuList,
			)
		}
		// Ranges are inclusive on both limits, so we have to add 1.
		// e.g. range 6-10 means CPUs 6, 7, 8, 9 and 10 (5, in total)
		// are available: (10 - 6) + 1 = 5.
		cpus += (upper - lower) + 1
	}
	return uint64(cpus), nil
}

func readCgroupFile(fs afero.Fs, v1Subpath, v2Subpath string) (string, error) {
	path := cgroupBaseDir
	subPath := v1Subpath
	v2CgroupPath, err := v2CgroupPath(fs)
	if err != nil {
		return "", err
	}
	if v2CgroupPath != "" {
		path = v2CgroupPath
		subPath = v2Subpath
	}
	filePath, err := recursiveCgroupsLookup(fs, path, subPath)
	if err != nil {
		return "", err
	}
	ls, err := utils.ReadFileLines(fs, filePath)
	if err != nil {
		return "", err
	}
	if len(ls) < 1 {
		return "", fmt.Errorf(
			"no value found in %s",
			filePath,
		)
	}
	return ls[0], nil
}

func v2CgroupPath(fs afero.Fs) (string, error) {
	ls, err := utils.ReadFileLines(fs, "/proc/self/cgroup")
	if err != nil {
		return "", err
	}
	if len(ls) < 1 {
		return "", errors.New("no cgroup data found for the current process")
	}
	// for a V2-only system, we expect exactly one line:
	// 0::<abs-path-to-cgroup>
	if strings.HasPrefix(ls[0], "0") {
		return cgroupBaseDir + ls[0][3:], nil
	}
	// This is either a v1 system, or system configured with a hybrid of
	// v1 & v2.
	return "", nil
}

func recursiveCgroupsLookup(fs afero.Fs, path, subPath string) (string, error) {
	parentDir := filepath.Dir(path)
	fullPath := filepath.Join(path, subPath)
	_, err := fs.Stat(fullPath)
	if err != nil {
		if os.IsNotExist(err) && path != cgroupBaseDir {
			return recursiveCgroupsLookup(fs, parentDir, subPath)
		}
		return "", err
	}
	return fullPath, nil
}
