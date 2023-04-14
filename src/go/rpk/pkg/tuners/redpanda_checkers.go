// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build !windows

package tuners

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/gcp"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/filesystem"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
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
	BallastFileChecker
)

func NewConfigChecker(conf *config.Config) Checker {
	return NewEqualityChecker(
		ConfigFileChecker,
		"Config file valid",
		Fatal,
		true,
		func() (interface{}, error) {
			ok, errs := conf.Check()
			var err error
			if len(errs) > 0 {
				s := multierror.ListFormatFunc(errs)
				err = fmt.Errorf("config file checker error: %v", s)
			}

			return ok, err
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
			memPerCPU := availableMem / int(effCpus)
			return memPerCPU, nil
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

func NewBallastFileChecker(fs afero.Fs, conf *config.Config) Checker {
	path := config.DefaultBallastFilePath
	if conf.Rpk.Tuners.BallastFilePath != "" {
		path = conf.Rpk.Tuners.BallastFilePath
	}
	return NewFileExistanceChecker(
		fs,
		IoConfigFileChecker,
		"Ballast file present",
		Warning,
		path,
	)
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
	schedulerChecker := NewDirectorySchedulerChecker(config.Redpanda.Directory, deviceFeatures, blockDevices)
	nomergesChecker := NewDirectoryNomergesChecker(config.Redpanda.Directory, deviceFeatures, blockDevices)
	balanceService := irq.NewBalanceService(fs, proc, executor, timeout)
	cpuMasks := irq.NewCPUMasks(fs, hwloc.NewHwLocCmd(proc, timeout), executor)
	dirIRQAffinityChecker := NewDirectoryIRQAffinityChecker(config.Redpanda.Directory, "all", irq.Default, blockDevices, cpuMasks)
	dirIRQAffinityStaticChecker := NewDirectoryIRQsAffinityStaticChecker(config.Redpanda.Directory, blockDevices, balanceService)
	if len(config.Redpanda.KafkaAPI) == 0 {
		return nil, errors.New("'redpanda.kafka_api' is empty")
	}
	interfaces, err := net.GetInterfacesByIps(
		config.Redpanda.KafkaAPI[0].Address,
		config.Redpanda.RPCServer.Address,
	)
	if err != nil {
		return nil, err
	}
	netCheckersFactory := NewNetCheckersFactory(
		fs, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks)
	checkers := map[CheckerID][]Checker{
		ConfigFileChecker:             {NewConfigChecker(config)},
		IoConfigFileChecker:           {NewIOConfigFileExistanceChecker(fs, ioConfigFile)},
		FreeMemChecker:                {NewMemoryChecker(fs)},
		SwapChecker:                   {NewSwapChecker(fs)},
		DataDirAccessChecker:          {NewDataDirWritableChecker(fs, config.Redpanda.Directory)},
		DiskSpaceChecker:              {NewFreeDiskSpaceChecker(config.Redpanda.Directory)},
		FsTypeChecker:                 {NewFilesystemTypeChecker(config.Redpanda.Directory)},
		TransparentHugePagesChecker:   {NewTransparentHugePagesChecker(fs)},
		NtpChecker:                    {NewNTPSyncChecker(timeout, fs)},
		SchedulerChecker:              {schedulerChecker},
		NomergesChecker:               {nomergesChecker},
		DiskIRQsAffinityChecker:       {dirIRQAffinityChecker},
		DiskIRQsAffinityStaticChecker: {dirIRQAffinityStaticChecker},
		FstrimChecker:                 {NewFstrimChecker()},
		SynBacklogChecker:             {netCheckersFactory.NewSynBacklogChecker()},
		ListenBacklogChecker:          {netCheckersFactory.NewListenBacklogChecker()},
		RfsTableEntriesChecker:        {netCheckersFactory.NewRfsTableSizeChecker()},
		NicIRQsAffinitStaticChecker:   {netCheckersFactory.NewNicIRQAffinityStaticChecker(interfaces)},
		NicIRQsAffinitChecker:         netCheckersFactory.NewNicIRQAffinityCheckers(interfaces, irq.Default, "all"),
		NicRpsChecker:                 netCheckersFactory.NewNicRpsSetCheckers(interfaces, irq.Default, "all"),
		NicRfsChecker:                 netCheckersFactory.NewNicRfsCheckers(interfaces),
		NicXpsChecker:                 netCheckersFactory.NewNicXpsCheckers(interfaces),
		MaxAIOEvents:                  {NewMaxAIOEventsChecker(fs)},
		ClockSource:                   {NewClockSourceChecker(fs)},
		Swappiness:                    {NewSwappinessChecker(fs)},
		KernelVersion:                 {NewKernelVersionChecker(GetKernelVersion)},
		BallastFileChecker:            {NewBallastFileChecker(fs, config)},
	}

	v, err := cloud.AvailableVendor()
	// NOTE: important workaround for very high flush latency in
	//       GCP when using local SSD's
	gcpVendor := gcp.GcpVendor{}
	if err == nil && v.Name() == gcpVendor.Name() {
		checkers[WriteCachePolicyChecker] = []Checker{NewDirectoryWriteCacheChecker(config.Redpanda.Directory, deviceFeatures, blockDevices)}
	}

	return checkers, nil
}
