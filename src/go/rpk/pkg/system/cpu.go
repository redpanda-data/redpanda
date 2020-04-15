package system

import "github.com/shirou/gopsutil/cpu"

func CpuInfo() ([]cpu.InfoStat, error) {
	return cpu.Info()
}
