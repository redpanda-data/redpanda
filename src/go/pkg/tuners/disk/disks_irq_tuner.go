package disk

import (
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/irq"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type diskType string

const (
	nonNvme diskType = "non-nvme"
	nvme    diskType = "nvme"
)

type devicesIRQs struct {
	devices []string
	irqs    []int
}

type disksIRQsTuner struct {
	fs                afero.Fs
	irqDeviceInfo     irq.DeviceInfo
	cpuMasks          irq.CpuMasks
	irqBalanceService irq.BalanceService
	irqProcFile       irq.ProcFile
	blockDevices      BlockDevices
	mode              irq.Mode
	baseCPUMask       string
	directories       []string
	devices           []string
	numberOfCpus      int
}

func NewDiskIRQTuner(
	fs afero.Fs,
	mode irq.Mode,
	cpuMask string,
	dirs []string,
	devices []string,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CpuMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	blockDevices BlockDevices,
	numberOfCpus int,
) tuners.Tunable {
	log.Debugf("Creating disk IRQs tuner with mode '%s', cpu mask '%s', directories '%s' and devices '%s'",
		mode, cpuMask, dirs, devices)

	return &disksIRQsTuner{
		fs:                fs,
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          cpuMasks,
		irqBalanceService: irqBalanceService,
		irqProcFile:       irqProcFile,
		blockDevices:      blockDevices,
		mode:              mode,
		baseCPUMask:       cpuMask,
		directories:       dirs,
		devices:           devices,
		numberOfCpus:      numberOfCpus,
	}
}

func (tuner *disksIRQsTuner) CheckIfSupported() (
	supported bool,
	reason string,
) {
	if len(tuner.directories) == 0 && len(tuner.directories) == 0 {
		return false, "Directories or devices are required for Disks IRQs Tuner"
	}
	if !tuner.cpuMasks.IsSupported() {
		return false, `Unable to calculate CPU masks required for IRQs tuner.
		 Please install 'hwloc'`
	}
	return true, ""
}

func (tuner *disksIRQsTuner) Tune() tuners.TuneResult {
	directoryDevices, err := tuner.blockDevices.GetDirectoriesDevices(
		tuner.directories)
	if err != nil {
		return tuners.NewTuneError(err)
	}

	var allDevices []string
	allDevices = append(allDevices, tuner.devices...)
	for _, devices := range directoryDevices {
		allDevices = append(allDevices, devices...)
	}
	balanceServiceTuner := NewDiskIRQsBalanceServiceTuner(
		tuner.fs,
		allDevices,
		tuner.blockDevices,
		tuner.irqBalanceService)

	if result := balanceServiceTuner.Tune(); result.IsFailed() {
		return result
	}
	affinityTuner := NewDiskIRQsAffinityTuner(
		tuner.fs,
		allDevices,
		tuner.baseCPUMask,
		tuner.mode,
		tuner.blockDevices,
		tuner.cpuMasks,
	)
	return affinityTuner.Tune()
}

func NewDiskIRQsBalanceServiceTuner(
	fs afero.Fs,
	devices []string,
	blockDevices BlockDevices,
	balanceService irq.BalanceService,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewDisksIRQAffinityStaticChecker(fs, devices, blockDevices, balanceService),
		func() tuners.TuneResult {
			deviceIRQs, err := blockDevices.GetDevicesIRQs(devices)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			err = balanceService.BanIRQsAndRestart(irq.GetAllIRQs(deviceIRQs))
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

func NewDiskIRQsAffinityTuner(
	fs afero.Fs,
	devices []string,
	cpuMask string,
	mode irq.Mode,
	blockDevices BlockDevices,
	cpuMasks irq.CpuMasks,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		NewDisksIRQAffinityChecker(fs, devices, cpuMask, mode, blockDevices, cpuMasks),
		func() tuners.TuneResult {
			distribution, err := GetExpectedIRQsDistribution(
				devices,
				blockDevices,
				mode,
				cpuMask,
				cpuMasks)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			err = cpuMasks.DistributeIRQs(distribution)
			if err != nil {
				return tuners.NewTuneError(err)
			}
			return tuners.NewTuneResult(false)
		},
		func() (bool, string) {
			if !cpuMasks.IsSupported() {
				return false, "Unable to calculate CPU masks required for IRQs " +
					"tuner. Please install 'hwloc'"
			}
			return true, ""
		},
	)
}

func GetExpectedIRQsDistribution(
	devices []string,
	blockDevices BlockDevices,
	mode irq.Mode,
	cpuMask string,
	cpuMasks irq.CpuMasks,
) (map[int]string, error) {
	log.Debugf("Getting %v IRQs distribution with mode %s and CPU mask %s",
		devices,
		mode, cpuMask)
	finalCpuMask, err := cpuMasks.BaseCpuMask(cpuMask)
	deviceIRQs, err := blockDevices.GetDevicesIRQs(devices)
	if err != nil {
		return nil, err
	}
	diskInfoByType, err := blockDevices.GroupDiskInfoByType(deviceIRQs)
	if err != nil {
		return nil, err
	}

	var effectiveMode irq.Mode
	if mode != irq.Default {
		effectiveMode = mode
	} else {
		effectiveMode, err = GetDefaultMode(finalCpuMask, diskInfoByType, cpuMasks)
		if err != nil {
			return nil, err
		}
	}

	nonNvmeDisksInfo := diskInfoByType[nonNvme]
	nvmeDisksInfo := diskInfoByType[nvme]
	irqCPUMask, err := cpuMasks.CpuMaskForIRQs(effectiveMode, finalCpuMask)
	if err != nil {
		return nil, err
	}
	devicesIRQsDistribution := make(map[int]string)
	if len(nonNvmeDisksInfo.devices) > 0 {
		IRQsDist, err := cpuMasks.GetIRQsDistributionMasks(
			nonNvmeDisksInfo.irqs, irqCPUMask)
		if err != nil {
			return nil, err
		}
		for IRQ, mask := range IRQsDist {
			devicesIRQsDistribution[IRQ] = mask
		}
	}

	if len(nvmeDisksInfo.devices) > 0 {
		IRQsDist, err := cpuMasks.GetIRQsDistributionMasks(
			nvmeDisksInfo.irqs, finalCpuMask)
		if err != nil {
			return nil, err
		}
		for IRQ, mask := range IRQsDist {
			devicesIRQsDistribution[IRQ] = mask
		}
	}
	log.Debugf("Calculated IRQs distribution %v", devicesIRQsDistribution)
	return devicesIRQsDistribution, nil
}

func GetDefaultMode(
	cpuMask string,
	diskInfoByType map[diskType]devicesIRQs,
	cpuMasks irq.CpuMasks,
) (irq.Mode, error) {

	log.Debug("Calculating default mode for Disk IRQs")
	nonNvmeDiskIRQs := diskInfoByType[nonNvme]
	if len(nonNvmeDiskIRQs.devices) == 0 {
		return irq.Mq, nil
	}
	numOfCores, err := cpuMasks.GetNumberOfCores(cpuMask)
	numOfPUs, err := cpuMasks.GetNumberOfPUs(cpuMask)
	if err != nil {
		return "", nil
	}
	log.Debugf("Considering '%d' cores and '%d' PUs", numOfCores, numOfPUs)
	if numOfPUs <= 4 {
		return irq.Mq, nil
	} else if numOfCores <= 4 {
		return irq.Sq, nil
	} else {
		return irq.SqSplit, nil
	}
}
