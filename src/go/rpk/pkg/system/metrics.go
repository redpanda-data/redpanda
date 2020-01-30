package system

import (
	"fmt"
	"strconv"
	"time"
	"vectorized/pkg/config"
	"vectorized/pkg/utils"

	"github.com/shirou/gopsutil/disk"
	"github.com/shirou/gopsutil/mem"
	"github.com/shirou/gopsutil/process"
	"github.com/spf13/afero"
)

type Metrics struct {
	CpuPercentage float64
	FreeMemory    uint64
	FreeSpace     uint64
}

func GatherMetrics(
	fs afero.Fs, timeout time.Duration, conf config.Config,
) (*Metrics, []error) {
	metrics := &Metrics{}
	errs := []error{}
	cpuPercentage, err := redpandaCpuPercentage(fs, conf.PidFile)
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.CpuPercentage = cpuPercentage
	}
	memInfo, err := mem.VirtualMemory()
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.FreeMemory = memInfo.Available
	}
	diskInfo, err := disk.Usage(conf.Redpanda.Directory)
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.FreeSpace = diskInfo.Free
	}

	return metrics, errs
}

func redpandaCpuPercentage(fs afero.Fs, pidFile string) (float64, error) {
	lines, err := utils.ReadFileLines(fs, pidFile)
	if err != nil {
		return 0, err
	}
	if len(lines) == 0 {
		return 0, fmt.Errorf("No PID present in %s", pidFile)
	}
	if len(lines) > 1 {
		return 0, fmt.Errorf("PID file corrupt: %s", pidFile)
	}
	pid, err := strconv.Atoi(lines[0])
	if err != nil {
		return 0, err
	}
	p, err := process.NewProcess(int32(pid))
	if err != nil {
		return 0, err
	}
	return p.CPUPercent()
}
