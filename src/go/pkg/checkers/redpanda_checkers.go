package checkers

import (
	"vectorized/redpanda"
	"vectorized/system"
	"vectorized/system/filesystem"

	"github.com/spf13/afero"
)

func NewConfigChecker(config *redpanda.Config) Checker {
	return NewEqualityChecker(
		"Config file valid",
		true,
		true,
		func() (interface{}, error) {
			return redpanda.CheckConfig(config), nil
		})
}

func NewDataDirWritableChecker(fs afero.Fs, path string) Checker {
	return NewEqualityChecker(
		"Data directory is writable",
		true,
		true,
		func() (interface{}, error) {
			return filesystem.DirectoryIsWriteable(fs, path)
		})
}

func NewFreeDiskSpaceChecker(path string) Checker {
	return NewFloatChecker(
		"Data partition free space [GB]",
		false,
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

func NewMemoryChecker() Checker {
	return NewIntChecker(
		"Free memory [MB]",
		true,
		func(current int) bool {
			return current >= 2048
		},
		func() string {
			return "2048"
		},
		system.GetMemAvailableMB,
	)
}

func NewFilesystemTypeChecker(path string) Checker {
	return NewEqualityChecker(
		"Data directory filesystem type",
		false,
		filesystem.Xfs,
		func() (interface{}, error) {
			return filesystem.GetFilesystemType(path)
		})
}

func NewIOConfigFileExistanceChecker(fs afero.Fs, filePath string) Checker {
	return NewFileExistanceChecker(
		fs,
		"I/O config file present",
		false,
		filePath)
}

func NewTransparentHugePagesChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		"Transparent huge pages active",
		false,
		true,
		func() (interface{}, error) {
			return system.GetTransparentHugePagesActive(fs)
		})
}

func NewNTPSyncChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		"NTP Synced",
		false,
		true,
		func() (interface{}, error) {
			ntpQuery := system.NewNtpQuery(fs)
			return ntpQuery.IsNtpSynced()
		},
	)
}

func RedpandaCheckers(
	fs afero.Fs, ioConfigFile string, config *redpanda.Config,
) []Checker {
	return []Checker{
		NewConfigChecker(config),
		NewMemoryChecker(),
		NewDataDirWritableChecker(fs, config.Directory),
		NewFreeDiskSpaceChecker(config.Directory),
		NewFilesystemTypeChecker(config.Directory),
		NewIOConfigFileExistanceChecker(fs, ioConfigFile),
		NewTransparentHugePagesChecker(fs),
		NewNTPSyncChecker(fs),
	}
}
