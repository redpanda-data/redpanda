// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package hwloc

import (
	"fmt"
	"regexp"
	"strings"
)

func TranslateToHwLocCPUSet(cpuset string) (string, error) {
	cpuSetPattern := regexp.MustCompile(`^(\d+-)?(\d+)(,(\d+-)?(\d+))*$`)
	if cpuset == "all" {
		return cpuset, nil
	}
	if !cpuSetPattern.MatchString(cpuset) {
		return "", fmt.Errorf("configured cpuset '%s' is invalid", cpuset)
	}
	var logicalCores []string
	for _, part := range strings.Split(cpuset, ",") {
		logicalCores = append(logicalCores, fmt.Sprintf("PU:%s", part))
	}
	return strings.Join(logicalCores, " "), nil
}
