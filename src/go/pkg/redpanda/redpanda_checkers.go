package redpanda

import (
	"time"
	"vectorized/pkg/checkers"
	"vectorized/pkg/config"
	"vectorized/pkg/net"
	"vectorized/pkg/os"
	"vectorized/pkg/system"
	"vectorized/pkg/system/filesystem"
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/ethtool"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/hwloc"
	"vectorized/pkg/tuners/irq"
	"vectorized/pkg/tuners/memory"
	"vectorized/pkg/tuners/network"
	"vectorized/pkg/tuners/sys"

	"github.com/spf13/afero"
)

type CheckerID int

const (
	ConfigFileChecker = iota
	DataDirAccessChecker
	DiskSpaceChecker
	FreeMemChecker
	SwapChecker
	FsTypeChecker
	IoConfigFileChecker
	TransparentHugePagesChecker
	NtpChecker
	SchedulerChecker
	NomergesChecker
	DiskIRQsAffinityStaticChecker
	DiskIRQsAffinityChecker
	NicIRQsAffinitChecker
	NicIRQsAffinitStaticChecker
	NicRfsChecker
	NicXpsChecker
	NicRpsChecker
	RfsTableEntriesChecker
	ListenBacklogChecker
	SynBacklogChecker
	MaxAIOEvents
	ClockSource
	Swappiness
)

func NewConfigChecker(conf *config.Config) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Config file valid",
		checkers.Fatal,
		true,
		func() (interface{}, error) {
			ok, _ := config.CheckConfig(conf)
			return ok, nil
		})
}

func NewDataDirWritableChecker(fs afero.Fs, path string) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Data directory is writable",
		checkers.Fatal,
		true,
		func() (interface{}, error) {
			return filesystem.DirectoryIsWriteable(fs, path)
		})
}

func NewFreeDiskSpaceChecker(path string) checkers.Checker {
	return checkers.NewFloatChecker(
		"Data partition free space [GB]",
		checkers.Warning,
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

func NewMemoryChecker(fs afero.Fs) checkers.Checker {
	return checkers.NewIntChecker(
		"Free memory [MB]",
		checkers.Warning,
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

func NewSwapChecker(fs afero.Fs) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Swap enabled",
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return system.IsSwapEnabled(fs)
		},
	)
}

func NewFilesystemTypeChecker(path string) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Data directory filesystem type",
		checkers.Warning,
		filesystem.Xfs,
		func() (interface{}, error) {
			return filesystem.GetFilesystemType(path)
		})
}

func NewIOConfigFileExistanceChecker(
	fs afero.Fs, filePath string,
) checkers.Checker {
	return checkers.NewFileExistanceChecker(
		fs,
		"I/O config file present",
		checkers.Warning,
		filePath)
}

func NewTransparentHugePagesChecker(fs afero.Fs) checkers.Checker {
	return checkers.NewEqualityChecker(
		"Transparent huge pages active",
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return system.GetTransparentHugePagesActive(fs)
		})
}

func NewNTPSyncChecker(timeout time.Duration, fs afero.Fs) checkers.Checker {
	return checkers.NewEqualityChecker(
		"NTP Synced",
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return system.NewNtpQuery(timeout, fs).IsNtpSynced()
		},
	)
}

func RedpandaCheckers(
	fs afero.Fs, ioConfigFile string, config *config.Config, timeout time.Duration,
) (map[CheckerID][]checkers.Checker, error) {
	proc := os.NewProc()
	ethtool, err := ethtool.NewEthtoolWrapper()
	if err != nil {
		return nil, err
	}
	executor := executors.NewDirectExecutor()
	irqProcFile := irq.NewProcFile(fs)
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	blockDevices := disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc, timeout)
	schedulerInfo := disk.NewSchedulerInfo(fs, blockDevices)
	schdulerCheckers, err := disk.NewDirectorySchedulerCheckers(fs,
		config.Redpanda.Directory, schedulerInfo, blockDevices)
	if err != nil {
		return nil, err
	}
	nomergesCheckers, err := disk.NewDirectoryNomergesCheckers(fs,
		config.Redpanda.Directory, schedulerInfo, blockDevices)
	if err != nil {
		return nil, err
	}
	balanceService := irq.NewBalanceService(fs, proc, executor, timeout)
	cpuMasks := irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc, timeout), executor)
	dirIRQAffinityChecker, err := disk.NewDirectoryIRQAffinityChecker(
		fs, config.Redpanda.Directory, "all", irq.Default, blockDevices, cpuMasks)
	if err != nil {
		return nil, err
	}
	dirIRQAffinityStaticChecker, err := disk.NewDirectoryIRQsAffinityStaticChecker(
		fs, config.Redpanda.Directory, blockDevices, balanceService)
	if err != nil {
		return nil, err
	}
	interfaces, err := net.GetInterfacesByIps(config.Redpanda.KafkaApi.Address, config.Redpanda.RPCServer.Address)
	if err != nil {
		return nil, err
	}
	netCheckersFactory := network.NewNetCheckersFactory(
		fs, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks)
	return map[CheckerID][]checkers.Checker{
		ConfigFileChecker:             []checkers.Checker{NewConfigChecker(config)},
		IoConfigFileChecker:           []checkers.Checker{NewIOConfigFileExistanceChecker(fs, ioConfigFile)},
		FreeMemChecker:                []checkers.Checker{NewMemoryChecker(fs)},
		SwapChecker:                   []checkers.Checker{NewSwapChecker(fs)},
		DataDirAccessChecker:          []checkers.Checker{NewDataDirWritableChecker(fs, config.Redpanda.Directory)},
		DiskSpaceChecker:              []checkers.Checker{NewFreeDiskSpaceChecker(config.Redpanda.Directory)},
		FsTypeChecker:                 []checkers.Checker{NewFilesystemTypeChecker(config.Redpanda.Directory)},
		TransparentHugePagesChecker:   []checkers.Checker{NewTransparentHugePagesChecker(fs)},
		NtpChecker:                    []checkers.Checker{NewNTPSyncChecker(timeout, fs)},
		SchedulerChecker:              schdulerCheckers,
		NomergesChecker:               nomergesCheckers,
		DiskIRQsAffinityChecker:       []checkers.Checker{dirIRQAffinityChecker},
		DiskIRQsAffinityStaticChecker: []checkers.Checker{dirIRQAffinityStaticChecker},
		SynBacklogChecker:             []checkers.Checker{netCheckersFactory.NewSynBacklogChecker()},
		ListenBacklogChecker:          []checkers.Checker{netCheckersFactory.NewListenBacklogChecker()},
		RfsTableEntriesChecker:        []checkers.Checker{netCheckersFactory.NewRfsTableSizeChecker()},
		NicIRQsAffinitStaticChecker:   []checkers.Checker{netCheckersFactory.NewNicIRQAffinityStaticChecker(interfaces)},
		NicIRQsAffinitChecker:         netCheckersFactory.NewNicIRQAffinityCheckers(interfaces, irq.Default, "all"),
		NicRpsChecker:                 netCheckersFactory.NewNicRpsSetCheckers(interfaces, irq.Default, "all"),
		NicRfsChecker:                 netCheckersFactory.NewNicRfsCheckers(interfaces),
		NicXpsChecker:                 netCheckersFactory.NewNicXpsCheckers(interfaces),
		MaxAIOEvents:                  []checkers.Checker{sys.NewMaxAIOEventsChecker(fs)},
		ClockSource:                   []checkers.Checker{sys.NewClockSourceChecker(fs)},
		Swappiness:                    []checkers.Checker{memory.NewSwappinessChecker(fs)},
	}, nil
}
