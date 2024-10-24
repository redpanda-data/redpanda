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
	"strconv"

	"github.com/lorenzosaino/go-sysctl"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/ethtool"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/irq"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/network"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type NetCheckersFactory interface {
	NewNicIRQAffinityStaticChecker(interfaces []string) Checker
	NewNicIRQAffinityCheckers(interfaces []string, mode irq.Mode, mask string) []Checker
	NewNicIRQAffinityChecker(nic network.Nic, mode irq.Mode, mask string) Checker
	NewNicRpsSetCheckers(interfaces []string, mode irq.Mode, mask string) []Checker
	NewNicRpsSetChecker(nic network.Nic, mode irq.Mode, mask string) Checker
	NewNicRfsCheckers(interfaces []string) []Checker
	NewNicRfsChecker(nic network.Nic) Checker
	NewNicNTupleCheckers(interfaces []string) []Checker
	NewNicNTupleChecker(nic network.Nic) Checker
	NewNicXpsCheckers(interfaces []string) []Checker
	NewNicXpsChecker(nic network.Nic) Checker
	NewRfsTableSizeChecker() Checker
	NewListenBacklogChecker() Checker
	NewSynBacklogChecker() Checker
}

type netCheckersFactory struct {
	fs             afero.Fs
	irqProcFile    irq.ProcFile
	irqDeviceInfo  irq.DeviceInfo
	ethtool        ethtool.EthtoolWrapper
	balanceService irq.BalanceService
	cpuMasks       irq.CPUMasks
}

func NewNetCheckersFactory(
	fs afero.Fs,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	balanceService irq.BalanceService,
	cpuMasks irq.CPUMasks,
) NetCheckersFactory {
	return &netCheckersFactory{
		fs:             fs,
		irqProcFile:    irqProcFile,
		irqDeviceInfo:  irqDeviceInfo,
		ethtool:        ethtool,
		balanceService: balanceService,
		cpuMasks:       cpuMasks,
	}
}

func (f *netCheckersFactory) NewNicIRQAffinityStaticChecker(
	interfaces []string,
) Checker {
	return NewEqualityChecker(
		NicIRQsAffinitStaticChecker,
		"NIC IRQs affinity static",
		Warning,
		true,
		func() (interface{}, error) {
			var IRQs []int
			for _, ifaceName := range interfaces {
				nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, ifaceName)
				nicIRQs, err := network.CollectIRQs(nic)
				if err != nil {
					return false, err
				}
				IRQs = append(IRQs, nicIRQs...)
			}
			return irq.AreIRQsStaticallyAssigned(IRQs, f.balanceService)
		},
	)
}

func (f *netCheckersFactory) NewNicIRQAffinityChecker(
	nic network.Nic, mode irq.Mode, cpuMask string,
) Checker {
	return NewEqualityChecker(
		NicIRQsAffinitChecker,
		fmt.Sprintf("NIC %s IRQ affinity set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				dist, err := network.GetHwInterfaceIRQsDistribution(
					currentNic, mode, cpuMask, f.cpuMasks)
				if err != nil {
					return false, err
				}
				for IRQ, mask := range dist {
					readMask, err := f.cpuMasks.ReadIRQMask(IRQ)
					if err != nil {
						return false, err
					}
					eq, err := irq.MasksEqual(readMask, mask)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
				}
				return true, nil
			})
		})
}

func (f *netCheckersFactory) NewNicIRQAffinityCheckers(
	interfaces []string, mode irq.Mode, cpuMask string,
) []Checker {
	return f.forNonVirtualInterfaces(interfaces,
		func(nic network.Nic) Checker {
			return f.NewNicIRQAffinityChecker(nic, mode, cpuMask)
		})
}

func (f *netCheckersFactory) NewNicRpsSetCheckers(
	interfaces []string, mode irq.Mode, cpuMask string,
) []Checker {
	return f.forNonVirtualInterfaces(
		interfaces,
		func(nic network.Nic) Checker {
			return f.NewNicRpsSetChecker(nic, mode, cpuMask)
		})
}

func (f *netCheckersFactory) NewNicRpsSetChecker(
	nic network.Nic, mode irq.Mode, cpuMask string,
) Checker {
	return NewEqualityChecker(
		NicRpsChecker,
		fmt.Sprintf("NIC %s RPS set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				rpsCPUs, err := currentNic.GetRpsCPUFiles()
				if err != nil {
					return false, err
				}
				rfsMask, err := network.GetRpsCPUMask(nic, mode, cpuMask, f.cpuMasks)
				if err != nil {
					return false, err
				}
				for _, rpsCPU := range rpsCPUs {
					readMask, err := f.cpuMasks.ReadMask(rpsCPU)
					if err != nil {
						return false, err
					}
					eq, err := irq.MasksEqual(readMask, rfsMask)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicRfsCheckers(interfaces []string) []Checker {
	return f.forNonVirtualInterfaces(interfaces, f.NewNicRfsChecker)
}

func (f *netCheckersFactory) NewNicRfsChecker(nic network.Nic) Checker {
	return NewEqualityChecker(
		NicRfsChecker,
		fmt.Sprintf("NIC %s RFS set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				limits, err := currentNic.GetRpsLimitFiles()
				queueLimit := network.OneRPSQueueLimit(limits)
				if err != nil {
					return false, err
				}
				for _, limitFile := range limits {
					setLimit, err := utils.ReadIntFromFile(f.fs, limitFile)
					if err != nil {
						return false, err
					}
					if setLimit != queueLimit {
						return false, nil
					}
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicNTupleCheckers(
	interfaces []string,
) []Checker {
	return f.forNonVirtualInterfaces(interfaces, f.NewNicNTupleChecker)
}

func (*netCheckersFactory) NewNicNTupleChecker(nic network.Nic) Checker {
	return NewEqualityChecker(
		NicNTupleChecker,
		fmt.Sprintf("NIC %s NTuple set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				nTupleStatus, err := currentNic.GetNTupleStatus()
				if err != nil {
					return false, err
				}
				if nTupleStatus == network.NTupleDisabled {
					return false, nil
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicXpsCheckers(interfaces []string) []Checker {
	return f.forNonVirtualInterfaces(interfaces, f.NewNicXpsChecker)
}

func (f *netCheckersFactory) NewNicXpsChecker(nic network.Nic) Checker {
	return NewEqualityChecker(
		NicXpsChecker,
		fmt.Sprintf("NIC %s XPS set", nic.Name()),
		Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic network.Nic) (bool, error) {
				xpsCPUFiles, err := currentNic.GetXpsCPUFiles()
				if err != nil {
					return false, err
				}
				masks, err := f.cpuMasks.GetDistributionMasks(uint(len(xpsCPUFiles)))
				if err != nil {
					return false, err
				}
				for i, mask := range masks {
					if exists, _ := afero.Exists(f.fs, xpsCPUFiles[i]); !exists {
						continue
					}
					readMask, err := f.cpuMasks.ReadMask(xpsCPUFiles[i])
					if err != nil {
						continue
					}
					eq, err := irq.MasksEqual(readMask, mask)
					if err != nil {
						return false, err
					}
					if !eq {
						return false, nil
					}
				}
				return true, nil
			})
		},
	)
}

func (*netCheckersFactory) NewRfsTableSizeChecker() Checker {
	return NewIntChecker(
		RfsTableEntriesChecker,
		"RFS Table entries",
		Warning,
		func(current int) bool {
			return current >= network.RfsTableSize
		},
		func() string {
			return fmt.Sprintf(">= %d", network.RfsTableSize)
		},
		func() (int, error) {
			value, err := sysctl.Get(network.RfsTableSizeProperty)
			if err != nil {
				return 0, err
			}
			return strconv.Atoi(value)
		},
	)
}

func (f *netCheckersFactory) NewListenBacklogChecker() Checker {
	return NewIntChecker(
		ListenBacklogChecker,
		"Connections listen backlog size",
		Warning,
		func(current int) bool {
			return current >= network.ListenBacklogSize
		},
		func() string {
			return fmt.Sprintf(">= %d", network.ListenBacklogSize)
		},
		func() (int, error) {
			return utils.ReadIntFromFile(f.fs, network.ListenBacklogFile)
		},
	)
}

func (f *netCheckersFactory) NewSynBacklogChecker() Checker {
	return NewIntChecker(
		SynBacklogChecker,
		"Max syn backlog size",
		Warning,
		func(current int) bool {
			return current >= network.SynBacklogSize
		},
		func() string {
			return fmt.Sprintf(">= %d", network.SynBacklogSize)
		},
		func() (int, error) {
			return utils.ReadIntFromFile(f.fs, network.SynBacklogFile)
		},
	)
}

func isSet(
	nic network.Nic, hwCheckFunction func(network.Nic) (bool, error),
) (bool, error) {
	if nic.IsHwInterface() {
		zap.L().Sugar().Debugf("'%s' is HW interface", nic.Name())
		return hwCheckFunction(nic)
	}
	if nic.IsBondIface() {
		zap.L().Sugar().Debugf("'%s' is bond interface", nic.Name())
		slaves, err := nic.Slaves()
		if err != nil {
			return false, err
		}
		for _, slave := range slaves {
			isSet, err := isSet(slave, hwCheckFunction)
			if err != nil {
				return false, err
			}
			if !isSet {
				return false, nil
			}
		}
	}
	return true, nil
}

func (f *netCheckersFactory) forNonVirtualInterfaces(
	interfaces []string, checkerFactory func(network.Nic) Checker,
) []Checker {
	var chkrs []Checker
	for _, iface := range interfaces {
		nic := network.NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, iface)
		if !nic.IsHwInterface() && !nic.IsBondIface() {
			zap.L().Sugar().Debugf("Skipping '%s' virtual interface", nic.Name())
			continue
		}
		chkrs = append(chkrs, checkerFactory(nic))
	}
	return chkrs
}
