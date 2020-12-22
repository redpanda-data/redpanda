// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"time"
	"vectorized/pkg/cloud"
	"vectorized/pkg/cloud/gcp"
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
	FstrimChecker
	NicIRQsAffinitChecker
	NicIRQsAffinitStaticChecker
	NicRfsChecker
	NicXpsChecker
	NicRpsChecker
	NicNTupleChecker
	RfsTableEntriesChecker
	ListenBacklogChecker
	SynBacklogChecker
	MaxAIOEvents
	ClockSource
	Swappiness
	KernelVersion
	WriteCachePolicyChecker
)

func NewConfigChecker(conf *config.Config) Checker {
	return NewEqualityChecker(
		ConfigFileChecker,
		"Config file valid",
		Fatal,
		true,
		func() (interface{}, error) {
			ok, _ := config.Check(conf)
			return ok, nil
		})
}

func NewDataDirWritableChecker(fs afero.Fs, path string) Checker {
	return NewEqualityChecker(
		DataDirAccessChecker,
		"Data directory is writable",
		Fatal,
		true,
		func() (interface{}, error) {
			return filesystem.DirectoryIsWriteable(fs, path)
		})
}

func NewFreeDiskSpaceChecker(path string) Checker {
	return NewFloatChecker(
		DiskSpaceChecker,
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
		FreeMemChecker,
		"Free memory per CPU [MB]",
		Warning,
		func(current int) bool {
			return current >= 2048
		},
		func() string {
			return "2048 per CPU"
		},
		func() (int, error) {
			effCpus, err := system.ReadCgroupEffectiveCpusNo(fs)
			if err != nil {
				return 0, err
			}
			availableMem, err := system.GetMemTotalMB(fs)
			if err != nil {
				return 0, err
			}
			memPerCpu := availableMem / int(effCpus)
			return memPerCpu, nil
		},
	)
}

func NewSwapChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		SwapChecker,
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
		FsTypeChecker,
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
		IoConfigFileChecker,
		"I/O config file present",
		Warning,
		filePath)
}

func NewNTPSyncChecker(timeout time.Duration, fs afero.Fs) Checker {
	return NewEqualityChecker(
		NtpChecker,
		"NTP Synced",
		Warning,
		true,
		func() (interface{}, error) {
			return system.NewNtpQuery(timeout, fs).IsNtpSynced()
		},
	)
}

func RedpandaCheckers(
	fs afero.Fs,
	ioConfigFile string,
	config *config.Config,
	timeout time.Duration,
) (map[CheckerID][]Checker, error) {
	proc := os.NewProc()
	ethtool, err := ethtool.NewEthtoolWrapper()
	if err != nil {
		return nil, err
	}
	executor := executors.NewDirectExecutor()
	irqProcFile := irq.NewProcFile(fs)
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	blockDevices := disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc, timeout)
	deviceFeatures := disk.NewDeviceFeatures(fs, blockDevices)
	schedulerChecker := NewDirectorySchedulerChecker(
		fs,
		config.Redpanda.Directory,
		deviceFeatures,
		blockDevices,
	)
	nomergesChecker := NewDirectoryNomergesChecker(
		fs,
		config.Redpanda.Directory,
		deviceFeatures,
		blockDevices,
	)
	balanceService := irq.NewBalanceService(fs, proc, executor, timeout)
	cpuMasks := irq.NewCpuMasks(fs, hwloc.NewHwLocCmd(proc, timeout), executor)
	dirIRQAffinityChecker := NewDirectoryIRQAffinityChecker(
		fs, config.Redpanda.Directory, "all", irq.Default, blockDevices, cpuMasks)
	dirIRQAffinityStaticChecker := NewDirectoryIRQsAffinityStaticChecker(
		fs,
		config.Redpanda.Directory,
		blockDevices,
		balanceService,
	)
	interfaces, err := net.GetInterfacesByIps(config.Redpanda.KafkaApi.Address, config.Redpanda.RPCServer.Address)
	if err != nil {
		return nil, err
	}
	netCheckersFactory := NewNetCheckersFactory(
		fs, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks)
	checkers := map[CheckerID][]Checker{
		ConfigFileChecker:             []Checker{NewConfigChecker(config)},
		IoConfigFileChecker:           []Checker{NewIOConfigFileExistanceChecker(fs, ioConfigFile)},
		FreeMemChecker:                []Checker{NewMemoryChecker(fs)},
		SwapChecker:                   []Checker{NewSwapChecker(fs)},
		DataDirAccessChecker:          []Checker{NewDataDirWritableChecker(fs, config.Redpanda.Directory)},
		DiskSpaceChecker:              []Checker{NewFreeDiskSpaceChecker(config.Redpanda.Directory)},
		FsTypeChecker:                 []Checker{NewFilesystemTypeChecker(config.Redpanda.Directory)},
		TransparentHugePagesChecker:   []Checker{NewTransparentHugePagesChecker(fs)},
		NtpChecker:                    []Checker{NewNTPSyncChecker(timeout, fs)},
		SchedulerChecker:              []Checker{schedulerChecker},
		NomergesChecker:               []Checker{nomergesChecker},
		DiskIRQsAffinityChecker:       []Checker{dirIRQAffinityChecker},
		DiskIRQsAffinityStaticChecker: []Checker{dirIRQAffinityStaticChecker},
		FstrimChecker:                 []Checker{NewFstrimChecker()},
		SynBacklogChecker:             []Checker{netCheckersFactory.NewSynBacklogChecker()},
		ListenBacklogChecker:          []Checker{netCheckersFactory.NewListenBacklogChecker()},
		RfsTableEntriesChecker:        []Checker{netCheckersFactory.NewRfsTableSizeChecker()},
		NicIRQsAffinitStaticChecker:   []Checker{netCheckersFactory.NewNicIRQAffinityStaticChecker(interfaces)},
		NicIRQsAffinitChecker:         netCheckersFactory.NewNicIRQAffinityCheckers(interfaces, irq.Default, "all"),
		NicRpsChecker:                 netCheckersFactory.NewNicRpsSetCheckers(interfaces, irq.Default, "all"),
		NicRfsChecker:                 netCheckersFactory.NewNicRfsCheckers(interfaces),
		NicXpsChecker:                 netCheckersFactory.NewNicXpsCheckers(interfaces),
		MaxAIOEvents:                  []Checker{NewMaxAIOEventsChecker(fs)},
		ClockSource:                   []Checker{NewClockSourceChecker(fs)},
		Swappiness:                    []Checker{NewSwappinessChecker(fs)},
		KernelVersion:                 []Checker{NewKernelVersionChecker(GetKernelVersion)},
	}

	v, err := cloud.AvailableVendor()
	// NOTE: important workaround for very high flush latency in
	//       GCP when using local SSD's
	gcpVendor := gcp.GcpVendor{}
	if err == nil && v.Name() == gcpVendor.Name() {
		checkers[WriteCachePolicyChecker] = []Checker{NewDirectoryWriteCacheChecker(fs,
			config.Redpanda.Directory,
			deviceFeatures,
			blockDevices)}
	}

	return checkers, nil
}
