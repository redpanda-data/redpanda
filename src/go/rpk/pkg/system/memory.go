package system

import (
	"github.com/docker/go-units"
	"github.com/spf13/afero"
)

type MemInfo struct {
	MemTotal       uint64
	MemFree        uint64
	CGroupMemLimit uint64
	SwapTotal      uint64
}

func GetTransparentHugePagesActive(fs afero.Fs) (bool, error) {

	options, err := ReadRuntineOptions(fs,
		"/sys/kernel/mm/transparent_hugepage/enabled")

	if err != nil {
		return false, err
	}

	if options.GetActive() != "never" {
		return true, nil
	}

	return false, nil
}

func GetMemTotalMB(fs afero.Fs) (int, error) {
	mInfo, err := getMemInfo(fs)
	if err != nil {
		return 0, err
	}

	memBytes := min(mInfo.MemTotal, mInfo.CGroupMemLimit)
	return int(memBytes / units.MiB), nil
}

func IsSwapEnabled(fs afero.Fs) (bool, error) {
	memInfo, err := getMemInfo(fs)
	if err != nil {
		return false, err
	}
	return memInfo.SwapTotal != 0, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
