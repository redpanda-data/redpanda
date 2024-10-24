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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

func NewNetTuner(
	mode irq.Mode,
	cpuMask string,
	interfaces []string,
	fs afero.Fs,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CPUMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	ethtool ethtool.EthtoolWrapper,
	executor executors.Executor,
) Tunable {
	factory := NewNetTunersFactory(
		fs, irqProcFile, irqDeviceInfo, ethtool, irqBalanceService, cpuMasks, executor)
	return NewAggregatedTunable(
		[]Tunable{
			factory.NewNICsBalanceServiceTuner(interfaces),
			factory.NewNICsIRQsAffinityTuner(interfaces, mode, cpuMask),
			factory.NewNICsRpsTuner(interfaces, mode, cpuMask),
			factory.NewNICsRfsTuner(interfaces),
			factory.NewNICsNTupleTuner(interfaces),
			factory.NewNICsXpsTuner(interfaces),
			factory.NewRfsTableSizeTuner(),
			factory.NewListenBacklogTuner(),
			factory.NewSynBacklogTuner(),
		})
}

type NetTunersFactory interface {
	NewNICsBalanceServiceTuner(interfaces []string) Tunable
	NewNICsIRQsAffinityTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewNICsRpsTuner(interfaces []string, mode irq.Mode, cpuMask string) Tunable
	NewNICsRfsTuner(interfaces []string) Tunable
	NewNICsNTupleTuner(interfaces []string) Tunable
	NewNICsXpsTuner(interfaces []string) Tunable
	NewRfsTableSizeTuner() Tunable
	NewListenBacklogTuner() Tunable
	NewSynBacklogTuner() Tunable
}

type netTunersFactory struct {
	fs              afero.Fs
	irqProcFile     irq.ProcFile
	irqDeviceInfo   irq.DeviceInfo
	ethtool         ethtool.EthtoolWrapper
	balanceService  irq.BalanceService
	cpuMasks        irq.CPUMasks
	checkersFactory NetCheckersFactory
	executor        executors.Executor
}

func NewNetTunersFactory(
	fs afero.Fs,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	balanceService irq.BalanceService,
	cpuMasks irq.CPUMasks,
	executor executors.Executor,
) NetTunersFactory {
	return &netTunersFactory{
		fs:             fs,
		irqProcFile:    irqProcFile,
		irqDeviceInfo:  irqDeviceInfo,
		ethtool:        ethtool,
		balanceService: balanceService,
		cpuMasks:       cpuMasks,
		executor:       executor,
		checkersFactory: NewNetCheckersFactory(
			fs, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks),
	}
}

func (f *netTunersFactory) NewNICsBalanceServiceTuner(
	interfaces []string,
) Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewNicIRQAffinityStaticChecker(interfaces),
		func() TuneResult {
			var IRQs []int
			for _, ifaceName := range interfaces {
				nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, ifaceName)
				nicIRQs, err := network.CollectIRQs(nic)
				if err != nil {
					return NewTuneError(err)
				}
				zap.L().Sugar().Debugf("%s interface IRQs: %v", nic.Name(), nicIRQs)
				IRQs = append(IRQs, nicIRQs...)
			}
			err := f.balanceService.BanIRQsAndRestart(IRQs)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) NewNICsIRQsAffinityTuner(
	interfaces []string, mode irq.Mode, cpuMask string,
) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicIRQAffinityChecker(nic, mode, cpuMask)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf("Tuning '%s' IRQs affinity", nic.Name())
			dist, err := network.GetHwInterfaceIRQsDistribution(nic, mode, cpuMask, f.cpuMasks)
			if err != nil {
				return NewTuneError(err)
			}
			f.cpuMasks.DistributeIRQs(dist)
			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsRpsTuner(
	interfaces []string, mode irq.Mode, cpuMask string,
) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRpsSetChecker(nic, mode, cpuMask)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf("Tuning '%s' RPS", nic.Name())
			rpsCPUs, err := nic.GetRpsCPUFiles()
			if err != nil {
				return NewTuneError(err)
			}
			rpsMask, err := network.GetRpsCPUMask(nic, mode, cpuMask, f.cpuMasks)
			if err != nil {
				return NewTuneError(err)
			}
			for _, rpsCPUFile := range rpsCPUs {
				err := f.cpuMasks.SetMask(rpsCPUFile, rpsMask)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsRfsTuner(interfaces []string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRfsChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf("Tuning '%s' RFS", nic.Name())
			limits, err := nic.GetRpsLimitFiles()
			if err != nil {
				return NewTuneError(err)
			}
			queueLimit := network.OneRPSQueueLimit(limits)
			for _, limitFile := range limits {
				err := f.writeIntToFile(limitFile, queueLimit)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsNTupleTuner(interfaces []string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicNTupleChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf("Tuning '%s' NTuple", nic.Name())
			ntupleFeature := map[string]bool{"ntuple": true}
			err := f.executor.Execute(
				commands.NewEthtoolChangeCmd(f.ethtool, nic.Name(), ntupleFeature))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsXpsTuner(interfaces []string) Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicXpsChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf("Tuning '%s' XPS", nic.Name())
			xpsCPUFiles, err := nic.GetXpsCPUFiles()
			if err != nil {
				return NewTuneError(err)
			}
			masks, err := f.cpuMasks.GetDistributionMasks(uint(len(xpsCPUFiles)))
			if err != nil {
				return NewTuneError(err)
			}
			for i, mask := range masks {
				err := f.cpuMasks.SetMask(xpsCPUFiles[i], mask)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewRfsTableSizeTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewRfsTableSizeChecker(),
		func() TuneResult {
			zap.L().Sugar().Debug("Tuning RFS table size")
			err := f.executor.Execute(
				commands.NewSysctlSetCmd(
					network.RfsTableSizeProperty, fmt.Sprint(network.RfsTableSize)))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) NewListenBacklogTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewListenBacklogChecker(),
		func() TuneResult {
			zap.L().Sugar().Debug("Tuning connections listen backlog size")
			err := f.writeIntToFile(network.ListenBacklogFile, network.ListenBacklogSize)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) NewSynBacklogTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewSynBacklogChecker(),
		func() TuneResult {
			zap.L().Sugar().Debug("Tuning SYN backlog size")
			err := f.writeIntToFile(network.SynBacklogFile, network.SynBacklogSize)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		f.executor.IsLazy(),
	)
}

func (f *netTunersFactory) writeIntToFile(file string, value int) error {
	return f.executor.Execute(
		commands.NewWriteFileCmd(f.fs, file, fmt.Sprint(value)))
}

func (f *netTunersFactory) tuneNonVirtualInterfaces(
	interfaces []string,
	checkerCreator func(network.Nic) Checker,
	tuneAction func(network.Nic) TuneResult,
	supportedAction func() (bool, string),
) Tunable {
	var tunables []Tunable
	for _, iface := range interfaces {
		nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, iface)
		if !nic.IsHwInterface() && !nic.IsBondIface() {
			zap.L().Sugar().Debugf("Skipping tuning of '%s' virtual interface", nic.Name())
			continue
		}
		tunables = append(tunables, NewCheckedTunable(
			checkerCreator(nic),
			func() TuneResult {
				return tuneInterface(nic, tuneAction)
			},
			supportedAction,
			f.executor.IsLazy(),
		))
	}
	return NewAggregatedTunable(tunables)
}

func tuneInterface(
	nic network.Nic, tuneAction func(network.Nic) TuneResult,
) TuneResult {
	if nic.IsHwInterface() {
		return tuneAction(nic)
	}

	if nic.IsBondIface() {
		slaves, err := nic.Slaves()
		if err != nil {
			return NewTuneError(err)
		}
		for _, slave := range slaves {
			return tuneInterface(slave, tuneAction)
		}
	}

	return NewTuneResult(false)
}
