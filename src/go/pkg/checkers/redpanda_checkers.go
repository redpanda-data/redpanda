package checkers

import (
	"vectorized/redpanda"
	"vectorized/system"
	"vectorized/system/filesystem"

	"github.com/spf13/afero"
)

type CheckerID int

const (
	ConfigFile = iota
	DataDirAccess
	DiskSpace
	FreeMem
	Swap
	FsType
	IoConfigFile
	TransparentHugePages
	NTP
)

func NewConfigChecker(config *redpanda.Config) Checker {
	return NewEqualityChecker(
		"Config file valid",
		Fatal,
		true,
		func() (interface{}, error) {
			return redpanda.CheckConfig(config), nil
		})
}

func NewDataDirWritableChecker(fs afero.Fs, path string) Checker {
	return NewEqualityChecker(
		"Data directory is writable",
		Fatal,
		true,
		func() (interface{}, error) {
			return filesystem.DirectoryIsWriteable(fs, path)
		})
}

func NewFreeDiskSpaceChecker(path string) Checker {
	return NewFloatChecker(
		"Data partition free space [GB]",
		Warning,
		func(current float64) bool {
			return current >= 10.0
		},
		func() string {
			return ">= 10"
		},
		func() (float64, error) {
			return filesystem.GetFreeDiskSpaceGB(path)
		})
}

func NewMemoryChecker(fs afero.Fs) Checker {
	return NewIntChecker(
		"Free memory [MB]",
		Warning,
		func(current int) bool {
			return current >= 2048
		},
		func() string {
			return "2048"
		},
		func() (int, error) {
			return system.GetMemTotalMB(fs)
		},
	)
}

func NewSwapChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		"Swap enabled",
		Warning,
		true,
		func() (interface{}, error) {
			return system.IsSwapEnabled(fs)
		},
	)
}

func NewFilesystemTypeChecker(path string) Checker {
	return NewEqualityChecker(
		"Data directory filesystem type",
		Warning,
		filesystem.Xfs,
		func() (interface{}, error) {
			return filesystem.GetFilesystemType(path)
		})
}

func NewIOConfigFileExistanceChecker(fs afero.Fs, filePath string) Checker {
	return NewFileExistanceChecker(
		fs,
		"I/O config file present",
		Warning,
		filePath)
}

func NewTransparentHugePagesChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		"Transparent huge pages active",
		Warning,
		true,
		func() (interface{}, error) {
			return system.GetTransparentHugePagesActive(fs)
		})
}

func NewNTPSyncChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		"NTP Synced",
		Warning,
		true,
		func() (interface{}, error) {
			ntpQuery := system.NewNtpQuery(fs)
			return ntpQuery.IsNtpSynced()
		},
	)
}

func RedpandaCheckers(
	fs afero.Fs, ioConfigFile string, config *redpanda.Config,
) map[CheckerID]Checker {
	return map[CheckerID]Checker{
		ConfigFile:           NewConfigChecker(config),
		IoConfigFile:         NewIOConfigFileExistanceChecker(fs, ioConfigFile),
		FreeMem:              NewMemoryChecker(fs),
		Swap:                 NewSwapChecker(fs),
		DataDirAccess:        NewDataDirWritableChecker(fs, config.Directory),
		DiskSpace:            NewFreeDiskSpaceChecker(config.Directory),
		FsType:               NewFilesystemTypeChecker(config.Directory),
		TransparentHugePages: NewTransparentHugePagesChecker(fs),
		NTP:                  NewNTPSyncChecker(fs),
	}
}
