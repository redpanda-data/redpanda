// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux

package tuners

import (
	"errors"
	"fmt"
	"time"

	"github.com/hashicorp/go-multierror"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/gcp"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/netutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system/filesystem"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
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
	NicRxTxQueueCountChecker
	NicIRQBalanceChecker
	NicIRQsAffinitChecker
	NetTunerConfigFileChecker
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

func NewConfigChecker(y *config.RedpandaYaml) Checker {
	return NewEqualityChecker(
		ConfigFileChecker,
		"Config file valid",
		Fatal,
		true,
		func() (interface{}, error) {
			ok, errs := y.Check()
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
		Fatal,
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

func NewBallastFileChecker(fs afero.Fs, y *config.RedpandaYaml) Checker {
	path := config.DefaultBallastFilePath
	if y.Rpk.Tuners.BallastFilePath != "" {
		path = y.Rpk.Tuners.BallastFilePath
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
	y *config.RedpandaYaml,
	timeout time.Duration,
) (map[CheckerID][]Checker, error) {
	proc := osutil.NewProc()
	ethtool, err := ethtool.NewEthtoolWrapper()
	if err != nil {
		return nil, err
	}
	executor := executors.NewDirectExecutor()
	irqProcFile := irq.NewProcFile(fs)
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	blockDevices := disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc, timeout)
	deviceFeatures := disk.NewDeviceFeatures(fs, blockDevices)
	schedulerChecker := NewDirectorySchedulerChecker(y.Redpanda.Directory, deviceFeatures, blockDevices)
	nomergesChecker := NewDirectoryNomergesChecker(y.Redpanda.Directory, deviceFeatures, blockDevices)
	balanceService := irq.NewBalanceService(fs, proc, executor, timeout)
	cpuMasks := irq.NewCPUMasks(fs, hwloc.NewHwLocCmd(proc, timeout), executor)
	dirIRQAffinityChecker := NewDirectoryIRQAffinityChecker(y.Redpanda.Directory, "all", irq.Default, blockDevices, cpuMasks)
	dirIRQAffinityStaticChecker := NewDirectoryIRQsAffinityStaticChecker(y.Redpanda.Directory, blockDevices, balanceService)
	if len(y.Redpanda.KafkaAPI) == 0 {
		return nil, errors.New("'redpanda.kafka_api' is empty")
	}
	addrs := []string{y.Redpanda.RPCServer.Address}
	for _, address := range y.Redpanda.KafkaAPI {
		addrs = append(addrs, address.Address)
	}
	interfaces, err := netutil.GetInterfacesByIps(
		addrs...,
	)
	if err != nil {
		return nil, err
	}
	netCheckersFactory := NewNetCheckersFactory(
		fs, y.Rpk, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks)
	nics := network.MapInterfaces(interfaces, fs, irqProcFile, irqDeviceInfo, ethtool)
	effectiveConfig := network.EffectiveNicConfig{}
	if len(nics) > 0 {
		// just use the first nic for now.
		// TODO: Unify net checkers below to match how the tuner actually works
		effectiveConfig, err = network.GetEffectiveNicConfig(nics[0], irq.Default, "all", cpuMasks, y.Rpk)
		if err != nil {
			return nil, fmt.Errorf("unable to get effective NIC config for checker setup: %w", err)
		}
	}
	checkers := map[CheckerID][]Checker{
		ConfigFileChecker:             {NewConfigChecker(y)},
		IoConfigFileChecker:           {NewIOConfigFileExistanceChecker(fs, ioConfigFile)},
		FreeMemChecker:                {NewMemoryChecker(fs)},
		SwapChecker:                   {NewSwapChecker(fs)},
		DataDirAccessChecker:          {NewDataDirWritableChecker(fs, y.Redpanda.Directory)},
		DiskSpaceChecker:              {NewFreeDiskSpaceChecker(y.Redpanda.Directory)},
		FsTypeChecker:                 {NewFilesystemTypeChecker(y.Redpanda.Directory)},
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
		NicRxTxQueueCountChecker:      netCheckersFactory.NewNicRxTxQueueCountCheckers(nics, effectiveConfig),
		NicIRQBalanceChecker:          {netCheckersFactory.NewNicIRQBalanceChecker(nics)},
		NicIRQsAffinitChecker:         netCheckersFactory.NewNicIRQAffinityCheckers(nics, effectiveConfig),
		NicRpsChecker:                 netCheckersFactory.NewNicRpsSetCheckers(nics, effectiveConfig),
		NicRfsChecker:                 netCheckersFactory.NewNicRfsCheckers(nics, effectiveConfig),
		NicXpsChecker:                 netCheckersFactory.NewNicXpsCheckers(nics),
		MaxAIOEvents:                  {NewMaxAIOEventsChecker(fs)},
		ClockSource:                   {NewClockSourceChecker(fs)},
		Swappiness:                    {NewSwappinessChecker(fs)},
		KernelVersion:                 {NewKernelVersionChecker(GetKernelVersion)},
		BallastFileChecker:            {NewBallastFileChecker(fs, y)},
	}

	v, err := cloud.AvailableProviders()
	// NOTE: important workaround for very high flush latency in
	//       GCP when using local SSD's
	gcpProvider := gcp.GcpProvider{}
	if err == nil && v.Name() == gcpProvider.Name() {
		checkers[WriteCachePolicyChecker] = []Checker{NewDirectoryWriteCacheChecker(y.Redpanda.Directory, deviceFeatures, blockDevices)}
	}

	return checkers, nil
}
