package network

import (
	"fmt"
	"strconv"
	"strings"
	"vectorized/pkg/checkers"
	"vectorized/pkg/tuners/ethtool"
	"vectorized/pkg/tuners/irq"

	"github.com/lorenzosaino/go-sysctl"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type NetCheckersFactory interface {
	NewNicIRQAffinityStaticChecker(interfaces []string) checkers.Checker
	NewNicIRQAffinityCheckers(interfaces []string, mode irq.Mode, mask string) []checkers.Checker
	NewNicIRQAffinityChecker(nic Nic, mode irq.Mode, mask string) checkers.Checker
	NewNicRpsSetCheckers(interfaces []string, mode irq.Mode, mask string) []checkers.Checker
	NewNicRpsSetChecker(nic Nic, mode irq.Mode, mask string) checkers.Checker
	NewNicRfsCheckers(interfaces []string) []checkers.Checker
	NewNicRfsChecker(nic Nic) checkers.Checker
	NewNicNTupleCheckers(interfaces []string) []checkers.Checker
	NewNicNTupleChecker(nic Nic) checkers.Checker
	NewNicXpsCheckers(interfaces []string) []checkers.Checker
	NewNicXpsChecker(nic Nic) checkers.Checker
	NewRfsTableSizeChecker() checkers.Checker
	NewListenBacklogChecker() checkers.Checker
	NewSynBacklogChecker() checkers.Checker
}

type netCheckersFactory struct {
	fs             afero.Fs
	irqProcFile    irq.ProcFile
	irqDeviceInfo  irq.DeviceInfo
	ethtool        ethtool.EthtoolWrapper
	balanceService irq.BalanceService
	cpuMasks       irq.CpuMasks
}

func NewNetCheckersFactory(
	fs afero.Fs,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool ethtool.EthtoolWrapper,
	balanceService irq.BalanceService,
	cpuMasks irq.CpuMasks,
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
) checkers.Checker {
	return checkers.NewEqualityChecker(
		"NIC IRQs affinity static",
		checkers.Warning,
		true,
		func() (interface{}, error) {
			var IRQs []int
			for _, ifaceName := range interfaces {
				nic := NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, ifaceName)
				nicIRQs, err := collectIRQs(nic)
				if err != nil {
					return false, nil
				}
				IRQs = append(IRQs, nicIRQs...)
			}
			return irq.AreIRQsStaticallyAssigned(IRQs, f.balanceService)
		},
	)
}

func (f *netCheckersFactory) NewNicIRQAffinityChecker(
	nic Nic, mode irq.Mode, cpuMask string,
) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("NIC %s IRQ affinity set", nic.Name()),
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic Nic) (bool, error) {
				dist, err := getHwInterfaceIRQsDistribution(
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
) []checkers.Checker {
	return f.forNonVirtualInterfaces(interfaces,
		func(nic Nic) checkers.Checker {
			return f.NewNicIRQAffinityChecker(nic, mode, cpuMask)
		})
}

func (f *netCheckersFactory) NewNicRpsSetCheckers(
	interfaces []string, mode irq.Mode, cpuMask string,
) []checkers.Checker {
	return f.forNonVirtualInterfaces(
		interfaces,
		func(nic Nic) checkers.Checker {
			return f.NewNicRpsSetChecker(nic, mode, cpuMask)
		})
}

func (f *netCheckersFactory) NewNicRpsSetChecker(
	nic Nic, mode irq.Mode, cpuMask string,
) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("NIC %s RPS set", nic.Name()),
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic Nic) (bool, error) {
				rpsCPUs, err := currentNic.GetRpsCPUFiles()
				if err != nil {
					return false, err
				}
				rfsMask, err := getRpsCPUMask(nic, mode, cpuMask, f.cpuMasks)
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

func (f *netCheckersFactory) NewNicRfsCheckers(
	interfaces []string,
) []checkers.Checker {
	return f.forNonVirtualInterfaces(interfaces, f.NewNicRfsChecker)
}

func (f *netCheckersFactory) NewNicRfsChecker(nic Nic) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("NIC %s RFS set", nic.Name()),
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic Nic) (bool, error) {
				limits, err := currentNic.GetRpsLimitFiles()
				queueLimit := oneRPSQueueLimit(limits)
				if err != nil {
					return false, err
				}
				for _, limitFile := range limits {
					setLimit, err := readIntFromFile(f.fs, limitFile)
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
) []checkers.Checker {
	return f.forNonVirtualInterfaces(interfaces, f.NewNicNTupleChecker)
}

func (f *netCheckersFactory) NewNicNTupleChecker(nic Nic) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("NIC %s NTuple set", nic.Name()),
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic Nic) (bool, error) {
				nTupleStatus, err := currentNic.GetNTupleStatus()
				if err != nil {
					return false, err
				}
				if nTupleStatus == NTupleDisabled {
					return false, nil
				}
				return true, nil
			})
		},
	)
}

func (f *netCheckersFactory) NewNicXpsCheckers(
	interfaces []string,
) []checkers.Checker {
	return f.forNonVirtualInterfaces(interfaces, f.NewNicXpsChecker)
}

func (f *netCheckersFactory) NewNicXpsChecker(nic Nic) checkers.Checker {
	return checkers.NewEqualityChecker(
		fmt.Sprintf("NIC %s XPS set", nic.Name()),
		checkers.Warning,
		true,
		func() (interface{}, error) {
			return isSet(nic, func(currentNic Nic) (bool, error) {
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

func readIntFromFile(fs afero.Fs, file string) (int, error) {
	content, err := afero.ReadFile(fs, file)
	if err != nil {
		return 0, nil
	}
	return strconv.Atoi(strings.TrimSpace(string(content)))

}

func (f *netCheckersFactory) NewRfsTableSizeChecker() checkers.Checker {
	return checkers.NewEqualityChecker(
		"RFS Table entries",
		checkers.Warning,
		rfsTableSize,
		func() (interface{}, error) {
			value, err := sysctl.Get(rfsTableSizeProperty)
			if err != nil {
				return false, err
			}
			return strconv.Atoi(value)
		})
}

func (f *netCheckersFactory) NewListenBacklogChecker() checkers.Checker {
	return checkers.NewEqualityChecker(
		"Socket listen() backlog size",
		checkers.Warning,
		listenBacklogSize,
		func() (interface{}, error) {
			return readIntFromFile(f.fs, listenBacklogFile)
		})
}

func (f *netCheckersFactory) NewSynBacklogChecker() checkers.Checker {
	return checkers.NewEqualityChecker(
		"Max syn backlog size",
		checkers.Warning,
		synBacklogSize,
		func() (interface{}, error) {
			return readIntFromFile(f.fs, synBacklogFile)
		})
}

func isSet(nic Nic, hwCheckFunction func(Nic) (bool, error)) (bool, error) {
	if nic.IsHwInterface() {
		log.Debugf("'%s' is HW interface", nic.Name())
		return hwCheckFunction(nic)
	}
	if nic.IsBondIface() {
		log.Debugf("'%s' is bond interface", nic.Name())
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
	interfaces []string, checkerFactory func(Nic) checkers.Checker,
) []checkers.Checker {
	var chkrs []checkers.Checker
	for _, iface := range interfaces {
		nic := NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, iface)
		if !nic.IsHwInterface() && !nic.IsBondIface() {
			log.Debugf("Skipping '%s' virtual interface", nic.Name())
			continue
		}
		chkrs = append(chkrs, checkerFactory(nic))
	}
	return chkrs
}
