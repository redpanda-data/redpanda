package system

import (
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
	FreeMemoryMB  float64
	FreeSpaceMB   float64
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
		metrics.FreeMemoryMB = float64(memInfo.Available) / 1024.0
	}
	diskInfo, err := disk.Usage(conf.Redpanda.Directory)
	if err != nil {
		errs = append(errs, err)
	} else {
		metrics.FreeSpaceMB = float64(diskInfo.Free) / 1024.0
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
