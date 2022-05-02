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
	"strconv"
	"strings"

	"github.com/spf13/afero"
)

type CPUInfo struct {
	ModelName string
	Cores     int
}

func GetCPUInfo(fs afero.Fs) ([]*CPUInfo, error) {
	bytes, err := afero.ReadFile(fs, "/proc/cpuinfo")
	if err != nil {
		return nil, err
	}
	contents := string(bytes)
	if contents == "" {
		return nil, errors.New("/proc/cpuinfo is empty")
	}
	sections := strings.Split(contents, "\n\n")

	cpus := []*CPUInfo{}
	for _, s := range sections {
		if s == "" {
			continue
		}
		cpu := &CPUInfo{Cores: 1}
		sectionLines := strings.Split(s, "\n")
		for _, l := range sectionLines {
			row := strings.Split(l, ":")
			for i := 0; i < len(row); i++ {
				row[i] = strings.TrimSpace(row[i])
			}
			if len(row) < 2 {
				continue
			}
			k := row[0]
			v := row[1]

			switch k {
			case "model name":
				cpu.ModelName = v
			case "cpu cores":
				cores, err := strconv.Atoi(v)
				if err != nil {
					return nil, err
				}
				cpu.Cores = cores
			}
		}
		cpus = append(cpus, cpu)
	}
	return cpus, nil
}
