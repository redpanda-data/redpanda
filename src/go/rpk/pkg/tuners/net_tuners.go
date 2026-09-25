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
	"fmt"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/osutil"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/out"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	"github.com/spf13/afero"
	"go.uber.org/zap"
	"gopkg.in/yaml.v3"
)

type CpusetConfig struct {
	IrqMode            irq.Mode `yaml:"irq_mode,omitempty" json:"irq_mode"`
	RedpandaCpuset     string   `yaml:"redpanda_cpuset,omitempty" json:"redpanda_cpuset"`
	RedpandaCpusetSize int      `yaml:"redpanda_cpuset_size,omitempty" json:"redpanda_cpuset_size"`
	IrqCpuset          string   `yaml:"interrupts_cpuset,omitempty" json:"interrupts_cpuset"`
	IrqCpusetSize      int      `yaml:"interrupts_cpuset_size,omitempty" json:"interrupts_cpuset_size"`
}

type NodeTunerState struct {
	Cpusets *CpusetConfig `yaml:"cpusets,omitempty" json:"cpusets"`
}

type netTunable struct {
	f          *netTunersFactory
	interfaces []string
	mode       irq.Mode
	cpuMask    string
}

func (n *netTunable) checkAllNicsSameMode(nics []network.Nic) (*network.EffectiveNicConfig, error) {
	var currentEffectiveConfig *network.EffectiveNicConfig

	for _, iface := range nics {
		effectiveConfig, err := network.GetEffectiveNicConfig(iface, n.mode, n.cpuMask, n.f.cpuMasks, n.f.rnc)
		if err != nil {
			return nil, err
		}

		zap.L().Sugar().Debugf("interface %s: effective config: %v", iface.Name(), effectiveConfig)
		if currentEffectiveConfig == nil {
			currentEffectiveConfig = &effectiveConfig
		} else {
			if *currentEffectiveConfig != effectiveConfig {
				return nil, fmt.Errorf("interfaces %s has different effective configuration than the rest: %v vs %v",
					iface.Name(), *currentEffectiveConfig, effectiveConfig)
			}
		}
	}
	return currentEffectiveConfig, nil
}

func (n *netTunable) CheckIfSupported() (bool, string) {
	if !n.f.cpuMasks.IsSupported() {
		return false, "Tuner is not supported as 'hwloc' is not installed"
	}
	return true, ""
}

func (n *netTunable) Tune() TuneResult {
	genericTuners := []Tunable{
		n.f.NewRfsTableSizeTuner(),
		n.f.NewListenBacklogTuner(),
		n.f.NewSynBacklogTuner(),
	}

	nics := network.MapInterfaces(n.interfaces, n.f.fs, n.f.irqProcFile, n.f.irqDeviceInfo, n.f.ethtool)
	if len(nics) == 0 {
		zap.L().Sugar().Debugf("No physical or bond interfaces to tune, only running generic network tuners")
		return NewAggregatedTunable(genericTuners).Tune()
	}

	effectiveConfig, err := n.checkAllNicsSameMode(nics)
	if err != nil {
		return NewTuneError(err)
	}

	zap.L().Sugar().Debugf("Using effective config: %v", *effectiveConfig)

	nicTuners := []Tunable{
		// RX/TX queue count tuner should always be first in order as others will re-read queue counts
		n.f.NewRxTxQueueCountTuner(nics, *effectiveConfig),
		n.f.NewNICsBalanceServiceTuner(nics),
		n.f.NewNICsIRQsAffinityTuner(nics, *effectiveConfig),
		// Write out net tuner interrupt config once we have successfully tuned IRQ config
		n.f.NewInterruptConfigFileTuner(*effectiveConfig),
		n.f.NewNICsRpsTuner(nics, *effectiveConfig),
		n.f.NewNICsRfsTuner(nics, *effectiveConfig),
		n.f.NewNICsNTupleTuner(nics),
		n.f.NewNICsXpsTuner(nics),
	}

	return NewAggregatedTunable(append(nicTuners, genericTuners...)).Tune()
}

func NewNetTuner(
	mode irq.Mode,
	rnc config.RpkNodeConfig,
	cpuMask string,
	interfaces []string,
	fs afero.Fs,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CPUMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	ethtool ethtool.EthtoolWrapper,
	executor executors.Executor,
	proc osutil.Proc,
	statePath string,
) Tunable {
	factory := NewNetTunersFactory(
		fs, rnc, irqProcFile, irqDeviceInfo, ethtool, irqBalanceService, cpuMasks, executor, proc, statePath)

	return &netTunable{f: factory.(*netTunersFactory), interfaces: interfaces, mode: mode, cpuMask: cpuMask}
}

type NetTunersFactory interface {
	NewRxTxQueueCountTuner(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) Tunable
	NewNICsBalanceServiceTuner(interfaces []network.Nic) Tunable
	NewNICsIRQsAffinityTuner(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) Tunable
	NewInterruptConfigFileTuner(effectiveConfig network.EffectiveNicConfig) Tunable
	NewNICsRpsTuner(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) Tunable
	NewNICsRfsTuner(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) Tunable
	NewNICsNTupleTuner(interfaces []network.Nic) Tunable
	NewNICsXpsTuner(interfaces []network.Nic) Tunable
	NewRfsTableSizeTuner() Tunable
	NewListenBacklogTuner() Tunable
	NewSynBacklogTuner() Tunable
}

type netTunersFactory struct {
	fs              afero.Fs
	rnc             config.RpkNodeConfig
	irqProcFile     irq.ProcFile
	irqDeviceInfo   irq.DeviceInfo
	ethtool         ethtool.EthtoolWrapper
	balanceService  irq.BalanceService
	cpuMasks        irq.CPUMasks
	checkersFactory NetCheckersFactory
	executor        executors.Executor
	proc            osutil.Proc
	statePath       string
}

func NewNetTunersFactory(
	fs afero.Fs,
	rnc config.RpkNodeConfig,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	balanceService irq.BalanceService,
	cpuMasks irq.CPUMasks,
	executor executors.Executor,
	proc osutil.Proc,
	statePath string,
) NetTunersFactory {
	return &netTunersFactory{
		fs:             fs,
		rnc:            rnc,
		irqProcFile:    irqProcFile,
		irqDeviceInfo:  irqDeviceInfo,
		ethtool:        ethtool,
		balanceService: balanceService,
		cpuMasks:       cpuMasks,
		executor:       executor,
		proc:           proc,
		statePath:      statePath,
		checkersFactory: NewNetCheckersFactory(
			fs, rnc, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks),
	}
}

func (f *netTunersFactory) NewRxTxQueueCountTuner(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) Tunable {
	return f.tuneInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRxTxQueueCountChecker(nic, effectiveConfig)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' queue counts", nic.Name()))
			if !f.rnc.Tuners.GetAllowRxTxQueueTuner() {
				zap.L().Sugar().Debugf("Skipping RX/TX Queue Tuner as it's disabled by configuration")
				return NewTuneResult(false)
			}

			supportsIrqLowering, err := nic.SupportsRxTxQueueLowering()
			if err != nil {
				return NewTuneError(err)
			}
			if !supportsIrqLowering {
				zap.L().Sugar().Debugf("Skipping RX/TX Queue Tuner as using an unknown driver")
				return NewTuneResult(false)
			}

			_, targetChannels, err := network.GetCurrentAndTargetChannels(nic, effectiveConfig, f.ethtool)
			if err != nil {
				return NewTuneError(err)
			}

			err = f.executor.Execute(
				commands.NewEthtoolSetChannelCmd(f.ethtool, nic.Name(), targetChannels))
			if err != nil {
				return NewTuneError(err)
			}

			return NewTuneResult(false)
		},
	)
}

func (f *netTunersFactory) NewNICsBalanceServiceTuner(
	interfaces []network.Nic,
) Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewNicIRQBalanceChecker(interfaces),
		func() TuneResult {
			var IRQs []int
			for _, nic := range interfaces {
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
	interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig,
) Tunable {
	return f.tuneInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicIRQAffinityChecker(nic, effectiveConfig)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' IRQs affinity", nic.Name()))
			dist, err := network.GetHwInterfaceIRQsDistribution(nic, effectiveConfig, f.cpuMasks)
			if err != nil {
				return NewTuneError(err)
			}
			f.cpuMasks.DistributeIRQs(dist)
			return NewTuneResult(false)
		},
	)
}

func (f *netTunersFactory) getNetTunerConfig(networkConfig network.EffectiveNicConfig) (NodeTunerState, error) {
	// All the below transformations we could also do in rpk:start but we just
	// do them here to avoid having to invoke hwloc again then.

	redpandaCpusetListForm, err := f.cpuMasks.MaskToListFormat(networkConfig.ComputationsCPUMask)
	if err != nil {
		return NodeTunerState{}, err
	}
	irqCpusetListForm, err := f.cpuMasks.MaskToListFormat(networkConfig.IRQCPUMask)
	if err != nil {
		return NodeTunerState{}, err
	}

	config := NodeTunerState{
		Cpusets: &CpusetConfig{
			IrqMode:            networkConfig.Mode,
			RedpandaCpuset:     redpandaCpusetListForm,
			RedpandaCpusetSize: networkConfig.ComputationsCPUMaskSize,
			IrqCpuset:          irqCpusetListForm,
			IrqCpusetSize:      networkConfig.IRQCPUMaskSize,
		},
	}
	return config, nil
}

func maybeReadFile(fs afero.Fs, path string) ([]byte, error) {
	exists, err := afero.Exists(fs, path)
	if err != nil {
		return nil, err
	}
	if !exists {
		return nil, nil
	}
	content, err := afero.ReadFile(fs, path)
	if err != nil {
		return nil, err
	}
	return content, nil
}

func (f *netTunersFactory) NewInterruptConfigFileTuner(effectiveConfig network.EffectiveNicConfig) Tunable {
	tunable := checkedTunable{}
	tunable.tuneAction = func() TuneResult {
		zap.L().Sugar().Debugf(out.WithLogBanner("Creating tuner config file"))

		config, err := f.getNetTunerConfig(effectiveConfig)
		if err != nil {
			return NewTuneError(err)
		}

		statePath := network.DefaultNodeTunerStateFile
		if f.statePath != "" {
			statePath = f.statePath
		}

		if config.Cpusets.IrqMode == irq.Mq {
			// Only do in dedicated mode to keep legacy behaviour otherwise. Remove the file if it exists.
			zap.L().Sugar().Debugf("Not writing net tuner config as irq mode is %s", config.Cpusets.IrqMode)

			exists, err := afero.Exists(f.fs, statePath)
			if err != nil {
				return NewTuneError(err)
			}
			if exists {
				// Empty file to avoid rpk:start from using it
				err = f.executor.Execute(
					commands.NewWriteFileCmd(f.fs, statePath, ""))
				if err != nil {
					return NewTuneError(fmt.Errorf("failed to empty existing net tuner config file %s: %w", statePath, err))
				}
			}

			return NewTuneResult(false)
		}

		marshalled, err := yaml.Marshal(&config)
		if err != nil {
			return NewTuneError(err)
		}

		err = f.executor.Execute(
			commands.NewWriteFileCmd(f.fs, statePath, string(marshalled)))
		if err != nil {
			return NewTuneError(fmt.Errorf("failed to write to net tuner config file %s: %w", statePath, err))
		}

		return NewTuneResult(false)
	}
	tunable.checker = NewEqualityChecker(
		NetTunerConfigFileChecker,
		"Net tuner config file correct",
		Warning,
		true,
		func() (interface{}, error) {
			targetConfig, err := f.getNetTunerConfig(effectiveConfig)
			if err != nil {
				return false, err
			}

			statePath := network.DefaultNodeTunerStateFile
			if f.statePath != "" {
				statePath = f.statePath
			}

			data, err := maybeReadFile(f.fs, statePath)
			if err != nil {
				return false, err
			}

			if targetConfig.Cpusets.IrqMode == irq.Mq {
				// If in MQ mode we still need to run the tuner such that it empties the file if it exists and is not empty
				return len(data) == 0, nil
			}

			if len(data) == 0 {
				return false, nil
			}

			currentConfig := NodeTunerState{}
			err = yaml.Unmarshal(data, &currentConfig)
			if err != nil {
				return false, err
			}

			if currentConfig.Cpusets == nil || *currentConfig.Cpusets != *targetConfig.Cpusets {
				zap.L().Sugar().Debugf("Current config %+v is different than target %+v", currentConfig, targetConfig)
				return false, nil
			}

			return true, nil
		},
	)
	tunable.supportedAction = func() (bool, string) { return true, "" }
	tunable.disablePostTuneCheck = f.executor.IsLazy()

	return &tunable
}

func (f *netTunersFactory) NewNICsRpsTuner(
	interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig,
) Tunable {
	return f.tuneInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRpsSetChecker(nic, effectiveConfig)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' RPS", nic.Name()))
			rpsCPUs, err := nic.GetRpsCPUFiles()
			if err != nil {
				return NewTuneError(err)
			}
			rpsMask, err := network.GetRpsCPUMask(nic, effectiveConfig, f.rnc)
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
	)
}

func (f *netTunersFactory) NewNICsRfsTuner(interfaces []network.Nic, effectiveConfig network.EffectiveNicConfig) Tunable {
	return f.tuneInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicRfsChecker(nic, effectiveConfig)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' RFS", nic.Name()))
			limits, err := nic.GetRpsLimitFiles()
			if err != nil {
				return NewTuneError(err)
			}
			queueLimit, err := network.OneRPSQueueLimit(limits, nic, effectiveConfig, f.rnc)
			if err != nil {
				return NewTuneError(err)
			}
			for _, limitFile := range limits {
				err := f.writeIntToFile(limitFile, queueLimit)
				if err != nil {
					return NewTuneError(err)
				}
			}
			return NewTuneResult(false)
		},
	)
}

func (f *netTunersFactory) NewNICsNTupleTuner(interfaces []network.Nic) Tunable {
	return f.tuneInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicNTupleChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' NTuple", nic.Name()))
			ntupleFeature := map[string]bool{"ntuple": true}
			err := f.executor.Execute(
				commands.NewEthtoolChangeCmd(f.ethtool, nic.Name(), ntupleFeature))
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
	)
}

func (f *netTunersFactory) NewNICsXpsTuner(interfaces []network.Nic) Tunable {
	return f.tuneInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.checkersFactory.NewNicXpsChecker(nic)
		},
		func(nic network.Nic) TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning '%s' XPS", nic.Name()))
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
	)
}

func (f *netTunersFactory) NewRfsTableSizeTuner() Tunable {
	return NewCheckedTunable(
		f.checkersFactory.NewRfsTableSizeChecker(),
		func() TuneResult {
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning RFS table size"))
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
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning connections listen backlog size"))
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
			zap.L().Sugar().Debugf(out.WithLogBanner("Tuning SYN backlog size"))
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

func (f *netTunersFactory) tuneInterfaces(
	interfaces []network.Nic,
	checkerCreator func(network.Nic) Checker,
	tuneAction func(network.Nic) TuneResult,
) Tunable {
	var tunables []Tunable
	for _, iface := range interfaces {
		tunables = append(tunables, NewCheckedTunable(
			checkerCreator(iface),
			func() TuneResult {
				return tuneInterface(iface, tuneAction)
			},
			func() (bool, string) {
				return true, ""
			},
			f.executor.IsLazy(),
		))
	}
	return NewAggregatedTunable(tunables)
}

func tuneInterface(
	nic network.Nic, tuneAction func(network.Nic) TuneResult,
) TuneResult {
	return tuneAction(nic)
}
