package hwloc

import (
	"fmt"
	"regexp"
	"strings"
)

func TranslateToHwLocCpuSet(cpuset string) (string, error) {

	cpuSetPattern := regexp.MustCompile("^(\\d+-)?(\\d+)(,(\\d+-)?(\\d+))*$")
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
