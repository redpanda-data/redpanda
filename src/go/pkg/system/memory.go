package system

import (
	"strconv"
	"strings"

	"github.com/docker/go-units"
	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
)

type MemInfo struct {
	MemTotal       uint64
	MemFree        uint64
	CGroupMemLimit uint64
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

func getMemInfo(fs afero.Fs) (*MemInfo, error) {
	var si unix.Sysinfo_t
	err := unix.Sysinfo(&si)
	if err != nil {
		return nil, err
	}
	cGroupMemLimit, err := getCgroupMemLimitBytes(fs)
	return &MemInfo{
		MemTotal:       si.Totalram * uint64(si.Unit),
		MemFree:        si.Freeram * uint64(si.Unit),
		CGroupMemLimit: cGroupMemLimit,
	}, nil
}

func getCgroupMemLimitBytes(fs afero.Fs) (uint64, error) {
	fileContent, err := afero.ReadFile(fs,
		"/sys/fs/cgroup/memory/memory.limit_in_bytes")
	if err != nil {
		return 0, err
	}
	limit, err := strconv.ParseUint(
		strings.TrimSpace(string(fileContent)), 10, 64)
	if err != nil {
		return 0, err
	}
	return limit, nil
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}
