package network

import (
	"fmt"
	"vectorized/pkg/checkers"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/irq"

	"github.com/lorenzosaino/go-sysctl"
	"github.com/safchain/ethtool"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func NewNetTuner(
	mode irq.Mode,
	cpuMask string,
	interfaces []string,
	fs afero.Fs,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CpuMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	ethtool *ethtool.Ethtool,
) tuners.Tunable {
	factory := NewNetTunersFactory(
		fs, irqProcFile, irqDeviceInfo, ethtool, irqBalanceService, cpuMasks)
	return tuners.NewAggregatedTunable(
		[]tuners.Tunable{
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
	NewNICsBalanceServiceTuner(interfaces []string) tuners.Tunable
	NewNICsIRQsAffinityTuner(interfaces []string, mode irq.Mode, cpuMask string) tuners.Tunable
	NewNICsRpsTuner(interfaces []string, mode irq.Mode, cpuMask string) tuners.Tunable
	NewNICsRfsTuner(interfaces []string) tuners.Tunable
	NewNICsNTupleTuner(interfaces []string) tuners.Tunable
	NewNICsXpsTuner(interfaces []string) tuners.Tunable
	NewRfsTableSizeTuner() tuners.Tunable
	NewListenBacklogTuner() tuners.Tunable
	NewSynBacklogTuner() tuners.Tunable
}

type netTunersFactory struct {
	NetTunersFactory
	fs              afero.Fs
	irqProcFile     irq.ProcFile
	irqDeviceInfo   irq.DeviceInfo
	ethtool         *ethtool.Ethtool
	balanceService  irq.BalanceService
	cpuMasks        irq.CpuMasks
	checkersFactory NetCheckersFactory
}

func NewNetTunersFactory(
	fs afero.Fs,
	irqProcFile irq.ProcFile,
	irqDeviceInfo irq.DeviceInfo,
	ethtool *ethtool.Ethtool,
	balanceService irq.BalanceService,
	cpuMasks irq.CpuMasks,
) NetTunersFactory {
	return &netTunersFactory{
		fs:             fs,
		irqProcFile:    irqProcFile,
		irqDeviceInfo:  irqDeviceInfo,
		ethtool:        ethtool,
		balanceService: balanceService,
		cpuMasks:       cpuMasks,
		checkersFactory: NewNetCheckersFactory(
			fs, irqProcFile, irqDeviceInfo, ethtool, balanceService, cpuMasks),
	}
}

func (f *netTunersFactory) NewNICsBalanceServiceTuner(
	interfaces []string,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		f.checkersFactory.NewNicIRQAffinityStaticChecker(interfaces),
		func() tuners.TuneResult {
			var IRQs []int
			for _, ifaceName := range interfaces {
				nic := NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, ifaceName)
				nicIRQs, err := collectIRQs(nic)
				if err != nil {
					return tuners.NewTuneError(err)
				}
				log.Debugf("%s interface IRQs: %v", nic.Name(), nicIRQs)
				IRQs = append(IRQs, nicIRQs...)
			}
			err := f.balanceService.BanIRQsAndRestart(IRQs)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsIRQsAffinityTuner(
	interfaces []string, mode irq.Mode, cpuMask string,
) tuners.Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic Nic) checkers.Checker {
			return f.checkersFactory.NewNicIRQAffinityChecker(nic, mode, cpuMask)
		},
		func(nic Nic) tuners.TuneResult {
			log.Debugf("Tuning '%s' IRQs affinity", nic.Name())
			dist, err := getHwInterfaceIRQsDistribution(nic, mode, cpuMask, f.cpuMasks)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			err = f.cpuMasks.DistributeIRQs(dist)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwoloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsRpsTuner(
	interfaces []string, mode irq.Mode, cpuMask string,
) tuners.Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic Nic) checkers.Checker {
			return f.checkersFactory.NewNicRpsSetChecker(nic, mode, cpuMask)
		},
		func(nic Nic) tuners.TuneResult {
			log.Debugf("Tuning '%s' RPS", nic.Name())
			rpsCPUs, err := nic.GetRpsCPUFiles()
			if err != nil {
				return tuners.NewTuneError(err)
			}
			rpsMask, err := getRpsCPUMask(nic, mode, cpuMask, f.cpuMasks)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			for _, rpsCPUFile := range rpsCPUs {
				err := f.cpuMasks.SetMask(rpsCPUFile, rpsMask)
				if err != nil {
					return tuners.NewTuneError(err)
				}
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			if !f.cpuMasks.IsSupported() {
				return false, "Tuner is not supported as 'hwoloc' is not installed"
			}
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsRfsTuner(interfaces []string) tuners.Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic Nic) checkers.Checker {
			return f.checkersFactory.NewNicRfsChecker(nic)
		},
		func(nic Nic) tuners.TuneResult {
			log.Debugf("Tuning '%s' RFS", nic.Name())
			limits, err := nic.GetRpsLimitFiles()
			if err != nil {
				return tuners.NewTuneError(err)
			}
			queueLimit := oneRPSQueueLimit(limits)
			for _, limitFile := range limits {
				err := writeIntToFile(f.fs, limitFile, queueLimit)
				if err != nil {
					return tuners.NewTuneError(err)
				}
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsNTupleTuner(
	interfaces []string,
) tuners.Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic Nic) checkers.Checker {
			return f.checkersFactory.NewNicNTupleChecker(nic)
		},
		func(nic Nic) tuners.TuneResult {
			log.Debugf("Tuning '%s' NTuple", nic.Name())
			ntupleFeature := map[string]bool{"ntuple": true}
			err := f.ethtool.Change(nic.Name(), ntupleFeature)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewNICsXpsTuner(interfaces []string) tuners.Tunable {
	return f.tuneNonVirtualInterfaces(
		interfaces,
		func(nic Nic) checkers.Checker {
			return f.checkersFactory.NewNicXpsChecker(nic)
		},
		func(nic Nic) tuners.TuneResult {
			log.Debugf("Tuning '%s' XPS", nic.Name())
			xpsCPUFiles, err := nic.GetXpsCPUFiles()
			if err != nil {
				return tuners.NewTuneError(err)
			}
			masks, err := f.cpuMasks.GetDistributionMasks(uint(len(xpsCPUFiles)))
			if err != nil {
				return tuners.NewTuneError(err)
			}
			for i, mask := range masks {
				err := f.cpuMasks.SetMask(xpsCPUFiles[i], mask)
				if err != nil {
					return tuners.NewTuneError(err)
				}
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewRfsTableSizeTuner() tuners.Tunable {
	return tuners.NewCheckedTunable(
		f.checkersFactory.NewRfsTableSizeChecker(),
		func() tuners.TuneResult {
			log.Debug("Tuning RFS table size")
			err := sysctl.Set(rfsTableSizeProperty, fmt.Sprint(rfsTableSize))
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewListenBacklogTuner() tuners.Tunable {
	return tuners.NewCheckedTunable(
		f.checkersFactory.NewListenBacklogChecker(),
		func() tuners.TuneResult {
			log.Debug("Tuning connections listen backlog size")
			err := writeIntToFile(f.fs, listenBacklogFile, listenBacklogSize)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func (f *netTunersFactory) NewSynBacklogTuner() tuners.Tunable {
	return tuners.NewCheckedTunable(
		f.checkersFactory.NewSynBacklogChecker(),
		func() tuners.TuneResult {
			log.Debug("Tuning SYN backlog size")
			err := writeIntToFile(f.fs, synBacklogFile, synBacklogSize)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
	)
}

func writeIntToFile(fs afero.Fs, file string, value int) error {
	return afero.WriteFile(fs, file,
		[]byte(fmt.Sprintln(value)),
		0644)
}

func (f *netTunersFactory) tuneNonVirtualInterfaces(
	interfaces []string,
	checkerCreator func(Nic) checkers.Checker,
	tuneAction func(Nic) tuners.TuneResult,
	supportedAction func() (bool, string),
) tuners.Tunable {

	var tunables []tuners.Tunable
	for _, iface := range interfaces {
		nic := NewNic(f.fs, f.irqProcFile, f.irqDeviceInfo, f.ethtool, iface)
		if !nic.IsHwInterface() && !nic.IsBondIface() {
			log.Debugf("Skipping tuning of '%s' virtual interface", nic.Name())
			continue
		}
		tunables = append(tunables, tuners.NewCheckedTunable(
			checkerCreator(nic),
			func() tuners.TuneResult {
				return tuneInterface(nic, tuneAction)
			},
			supportedAction,
		))
	}
	return tuners.NewAggregatedTunable(tunables)
}

func tuneInterface(
	nic Nic, tuneAction func(Nic) tuners.TuneResult,
) tuners.TuneResult {
	if nic.IsHwInterface() {
		return tuneAction(nic)
	}

	if nic.IsBondIface() {
		slaves, err := nic.Slaves()
		if err != nil {
			return tuners.NewTuneError(err)
		}
		for _, slave := range slaves {
			return tuneInterface(slave, tuneAction)
		}
	}

	return tuners.NewTuneResult(false)
}
