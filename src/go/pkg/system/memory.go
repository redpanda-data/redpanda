package system

import (
	"github.com/docker/go-units"
	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
)

type MemInfo struct {
	MemTotal uint64
	MemFree  uint64
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

func GetMemTotalMB() (int, error) {
	mInfo, err := getMemInfo()
	if err != nil {
		return 0, err
	}
	return int(mInfo.MemTotal / units.MiB), nil
}

func getMemInfo() (*MemInfo, error) {
	var si unix.Sysinfo_t
	err := unix.Sysinfo(&si)
	if err != nil {
		return nil, err
	}
	return &MemInfo{
		MemTotal: si.Totalram * uint64(si.Unit),
		MemFree:  si.Freeram * uint64(si.Unit),
	}, nil
}
