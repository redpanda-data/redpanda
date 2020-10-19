package system

import (
	"syscall"

	"github.com/spf13/afero"
)

func getMemInfo(fs afero.Fs) (*MemInfo, error) {
	var si syscall.Sysinfo_t
	err := syscall.Sysinfo(&si)
	if err != nil {
		return nil, err
	}
	cGroupMemLimit, err := ReadCgroupMemLimitBytes(fs)
	if err != nil {
		return nil, err
	}
	return &MemInfo{
		MemTotal:       si.Totalram * uint64(si.Unit),
		MemFree:        si.Freeram * uint64(si.Unit),
		CGroupMemLimit: cGroupMemLimit,
		SwapTotal:      si.Totalswap * uint64(si.Unit),
	}, nil
}
