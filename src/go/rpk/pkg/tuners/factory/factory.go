// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package factory

import (
	"runtime"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/cloud/gcp"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/net"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ballast"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/coredump"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/cpu"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/disk"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/hwloc"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

var allTuners = map[string]func(*tunersFactory, *TunerParams) tuners.Tunable{
	"disk_irq":              (*tunersFactory).newDiskIRQTuner,
	"disk_scheduler":        (*tunersFactory).newDiskSchedulerTuner,
	"disk_nomerges":         (*tunersFactory).newDiskNomergesTuner,
	"disk_write_cache":      (*tunersFactory).newGcpWriteCacheTuner,
	"fstrim":                (*tunersFactory).newFstrimTuner,
	"net":                   (*tunersFactory).newNetworkTuner,
	"cpu":                   (*tunersFactory).newCPUTuner,
	"aio_events":            (*tunersFactory).newMaxAIOEventsTuner,
	"clocksource":           (*tunersFactory).newClockSourceTuner,
	"swappiness":            (*tunersFactory).newSwappinessTuner,
	"transparent_hugepages": (*tunersFactory).newTHPTuner,
	"coredump":              (*tunersFactory).newCoredumpTuner,
	"ballast_file":          (*tunersFactory).newBallastFileTuner,
}

type TunerParams struct {
	Mode          string
	CPUMask       string
	RebootAllowed bool
	Disks         []string
	Directories   []string
	Nics          []string
}

type TunersFactory interface {
	CreateTuner(tunerType string, params *TunerParams) tuners.Tunable
}

type tunersFactory struct {
	fs                afero.Fs
	conf              config.RpkNodeTuners
	irqDeviceInfo     irq.DeviceInfo
	cpuMasks          irq.CPUMasks
	irqBalanceService irq.BalanceService
	irqProcFile       irq.ProcFile
	blockDevices      disk.BlockDevices
	proc              os.Proc
	grub              system.Grub
	executor          executors.Executor
}

func NewDirectExecutorTunersFactory(fs afero.Fs, conf config.RpkNodeTuners, timeout time.Duration) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	executor := executors.NewDirectExecutor()
	return newTunersFactory(fs, conf, irqProcFile, proc, irqDeviceInfo, executor, timeout)
}

func NewScriptRenderingTunersFactory(fs afero.Fs, conf config.RpkNodeTuners, out string, timeout time.Duration) TunersFactory {
	irqProcFile := irq.NewProcFile(fs)
	proc := os.NewProc()
	irqDeviceInfo := irq.NewDeviceInfo(fs, irqProcFile)
	executor := executors.NewScriptRenderingExecutor(fs, out)
	return newTunersFactory(fs, conf, irqProcFile, proc, irqDeviceInfo, executor, timeout)
}

func newTunersFactory(
	fs afero.Fs,
	conf config.RpkNodeTuners,
	irqProcFile irq.ProcFile,
	proc os.Proc,
	irqDeviceInfo irq.DeviceInfo,
	executor executors.Executor,
	timeout time.Duration,
) TunersFactory {
	return &tunersFactory{
		fs:                fs,
		conf:              conf,
		irqProcFile:       irqProcFile,
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          irq.NewCPUMasks(fs, hwloc.NewHwLocCmd(proc, timeout), executor),
		irqBalanceService: irq.NewBalanceService(fs, proc, executor, timeout),
		blockDevices:      disk.NewBlockDevices(fs, irqDeviceInfo, irqProcFile, proc, timeout),
		grub:              system.NewGrub(os.NewCommands(proc), proc, fs, executor, timeout),
		proc:              proc,
		executor:          executor,
	}
}

func AvailableTuners() []string {
	var keys []string
	for key := range allTuners {
		keys = append(keys, key)
	}
	return keys
}

func IsTunerAvailable(tuner string) bool {
	return allTuners[tuner] != nil
}

func IsTunerEnabled(tuner string, tuneCfg config.RpkNodeTuners) bool {
	switch tuner {
	case "disk_irq":
		return tuneCfg.TuneDiskIrq
	case "disk_scheduler":
		return tuneCfg.TuneDiskScheduler
	case "disk_nomerges":
		return tuneCfg.TuneNomerges
	case "disk_write_cache":
		return tuneCfg.TuneDiskWriteCache
	case "fstrim":
		return tuneCfg.TuneFstrim
	case "net":
		return tuneCfg.TuneNetwork
	case "cpu":
		return tuneCfg.TuneCPU
	case "aio_events":
		return tuneCfg.TuneAioEvents
	case "clocksource":
		return tuneCfg.TuneClocksource
	case "swappiness":
		return tuneCfg.TuneSwappiness
	case "transparent_hugepages":
		return tuneCfg.TuneTransparentHugePages
	case "coredump":
		return tuneCfg.TuneCoredump
	case "ballast_file":
		return tuneCfg.TuneBallastFile
	}
	return false
}

func (factory *tunersFactory) CreateTuner(
	tunerName string, tunerParams *TunerParams,
) tuners.Tunable {
	return allTuners[tunerName](factory, tunerParams)
}

func (factory *tunersFactory) newDiskIRQTuner(
	params *TunerParams,
) tuners.Tunable {
	return tuners.NewDiskIRQTuner(
		factory.fs,
		irq.ModeFromString(params.Mode),
		params.CPUMask,
		params.Directories,
		params.Disks,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		factory.blockDevices,
		runtime.NumCPU(),
		factory.executor,
	)
}

func (factory *tunersFactory) newDiskSchedulerTuner(
	params *TunerParams,
) tuners.Tunable {
	return tuners.NewSchedulerTuner(
		factory.fs,
		params.Directories,
		params.Disks,
		factory.blockDevices,
		factory.executor,
	)
}

func (factory *tunersFactory) newDiskNomergesTuner(
	params *TunerParams,
) tuners.Tunable {
	return tuners.NewNomergesTuner(
		factory.fs,
		params.Directories,
		params.Disks,
		factory.blockDevices,
		factory.executor,
	)
}

func (factory *tunersFactory) newGcpWriteCacheTuner(
	params *TunerParams,
) tuners.Tunable {
	return tuners.NewGcpWriteCacheTuner(
		factory.fs,
		params.Directories,
		params.Disks,
		factory.blockDevices,
		&gcp.GcpVendor{},
		factory.executor,
	)
}

func (factory *tunersFactory) newFstrimTuner(_ *TunerParams) tuners.Tunable {
	return tuners.NewFstrimTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newNetworkTuner(
	params *TunerParams,
) tuners.Tunable {
	ethtool, err := ethtool.NewEthtoolWrapper()
	if err != nil {
		panic(err)
	}
	return tuners.NewNetTuner(
		irq.ModeFromString(params.Mode),
		params.CPUMask,
		params.Nics,
		factory.fs,
		factory.irqDeviceInfo,
		factory.cpuMasks,
		factory.irqBalanceService,
		factory.irqProcFile,
		ethtool,
		factory.executor,
	)
}

func (factory *tunersFactory) newCPUTuner(params *TunerParams) tuners.Tunable {
	return cpu.NewCPUTuner(
		factory.cpuMasks,
		factory.grub,
		factory.fs,
		params.RebootAllowed,
		factory.executor,
	)
}

func (factory *tunersFactory) newMaxAIOEventsTuner(
	_ *TunerParams,
) tuners.Tunable {
	return tuners.NewMaxAIOEventsTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newClockSourceTuner(
	_ *TunerParams,
) tuners.Tunable {
	return tuners.NewClockSourceTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newSwappinessTuner(
	_ *TunerParams,
) tuners.Tunable {
	return tuners.NewSwappinessTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newTHPTuner(_ *TunerParams) tuners.Tunable {
	return tuners.NewEnableTHPTuner(factory.fs, factory.executor)
}

func (factory *tunersFactory) newCoredumpTuner(_ *TunerParams) tuners.Tunable {
	return coredump.NewCoredumpTuner(factory.fs, factory.conf.CoredumpDir, factory.executor)
}

func (factory *tunersFactory) newBallastFileTuner(
	_ *TunerParams,
) tuners.Tunable {
	return ballast.NewBallastFileTuner(factory.conf.BallastFilePath, factory.conf.BallastFileSize, factory.executor)
}

func MergeTunerParamsConfig(params *TunerParams, conf *config.Config) (*TunerParams, error) {
	if len(params.Nics) == 0 {
		addrs := []string{conf.Redpanda.RPCServer.Address}
		if len(conf.Redpanda.KafkaAPI) > 0 {
			addrs = append(addrs, conf.Redpanda.KafkaAPI[0].Address)
		}
		nics, err := net.GetInterfacesByIps(
			addrs...,
		)
		if err != nil {
			return params, err
		}
		params.Nics = nics
	}
	if len(params.Directories) == 0 {
		params.Directories = []string{conf.Redpanda.Directory}
	}
	return params, nil
}

func FillTunerParamsWithValuesFromConfig(params *TunerParams, conf *config.Config) error {
	addrs := []string{conf.Redpanda.RPCServer.Address}
	if len(conf.Redpanda.KafkaAPI) > 0 {
		addrs = append(addrs, conf.Redpanda.KafkaAPI[0].Address)
	}
	nics, err := net.GetInterfacesByIps(
		addrs...,
	)
	if err != nil {
		return err
	}
	params.Nics = nics
	zap.L().Sugar().Debugf("Redpanda uses '%v' NICs", params.Nics)
	zap.L().Sugar().Debugf("Redpanda data directory '%s'", conf.Redpanda.Directory)
	params.Directories = []string{conf.Redpanda.Directory}
	return nil
}
