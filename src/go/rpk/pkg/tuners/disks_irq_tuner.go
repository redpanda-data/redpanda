package tuners

import (
	"vectorized/pkg/tuners/disk"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/irq"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type disksIRQsTuner struct {
	fs                afero.Fs
	irqDeviceInfo     irq.DeviceInfo
	cpuMasks          irq.CpuMasks
	irqBalanceService irq.BalanceService
	irqProcFile       irq.ProcFile
	blockDevices      disk.BlockDevices
	mode              irq.Mode
	baseCPUMask       string
	directories       []string
	devices           []string
	numberOfCpus      int
	executor          executors.Executor
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
	blockDevices disk.BlockDevices,
	numberOfCpus int,
	executor executors.Executor,
) Tunable {
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
		executor:          executor,
	}
}

func (tuner *disksIRQsTuner) CheckIfSupported() (
	supported bool,
	reason string,
) {
	if len(tuner.directories) == 0 && len(tuner.devices) == 0 {
		return false, "Directories or devices are required for Disks IRQs Tuner"
	}
	if !tuner.cpuMasks.IsSupported() {
		return false, `Unable to calculate CPU masks required for IRQs tuner.
		 Please install 'hwloc'`
	}
	return true, ""
}

func (tuner *disksIRQsTuner) Tune() TuneResult {
	directoryDevices, err := tuner.blockDevices.GetDirectoriesDevices(
		tuner.directories)
	if err != nil {
		return NewTuneError(err)
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
		tuner.irqBalanceService,
		tuner.executor)

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
		tuner.executor,
	)
	return affinityTuner.Tune()
}

func NewDiskIRQsBalanceServiceTuner(
	fs afero.Fs,
	devices []string,
	blockDevices disk.BlockDevices,
	balanceService irq.BalanceService,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDisksIRQAffinityStaticChecker(fs, devices, blockDevices, balanceService),
		func() TuneResult {
			diskInfoByType, err := blockDevices.GetDiskInfoByType(devices)
			if err != nil {
				return NewTuneError(err)
			}
			var IRQs []int
			for _, diskInfo := range diskInfoByType {
				IRQs = append(IRQs, diskInfo.Irqs...)
			}
			err = balanceService.BanIRQsAndRestart(IRQs)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		executor.IsLazy(),
	)
}

func NewDiskIRQsAffinityTuner(
	fs afero.Fs,
	devices []string,
	cpuMask string,
	mode irq.Mode,
	blockDevices disk.BlockDevices,
	cpuMasks irq.CpuMasks,
	executor executors.Executor,
) Tunable {
	return NewCheckedTunable(
		NewDisksIRQAffinityChecker(fs, devices, cpuMask, mode, blockDevices, cpuMasks),
		func() TuneResult {
			distribution, err := GetExpectedIRQsDistribution(
				devices,
				blockDevices,
				mode,
				cpuMask,
				cpuMasks)
			if err != nil {
				return NewTuneError(err)
			}
			err = cpuMasks.DistributeIRQs(distribution)
			if err != nil {
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			if !cpuMasks.IsSupported() {
				return false, "Unable to calculate CPU masks required for IRQs " +
					"tuner. Please install 'hwloc'"
			}
			return true, ""
		},
		executor.IsLazy(),
	)
}

func GetExpectedIRQsDistribution(
	devices []string,
	blockDevices disk.BlockDevices,
	mode irq.Mode,
	cpuMask string,
	cpuMasks irq.CpuMasks,
) (map[int]string, error) {
	log.Debugf("Getting %v IRQs distribution with mode %s and CPU mask %s",
		devices,
		mode, cpuMask)
	finalCpuMask, err := cpuMasks.BaseCpuMask(cpuMask)
	if err != nil {
		return nil, err
	}
	diskInfoByType, err := blockDevices.GetDiskInfoByType(devices)
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

	nonNvmeDisksInfo := diskInfoByType[disk.NonNvme]
	nvmeDisksInfo := diskInfoByType[disk.Nvme]
	irqCPUMask, err := cpuMasks.CpuMaskForIRQs(effectiveMode, finalCpuMask)
	if err != nil {
		return nil, err
	}
	devicesIRQsDistribution := make(map[int]string)
	if len(nonNvmeDisksInfo.Devices) > 0 {
		IRQsDist, err := cpuMasks.GetIRQsDistributionMasks(
			nonNvmeDisksInfo.Irqs, irqCPUMask)
		if err != nil {
			return nil, err
		}
		for IRQ, mask := range IRQsDist {
			devicesIRQsDistribution[IRQ] = mask
		}
	}

	if len(nvmeDisksInfo.Devices) > 0 {
		IRQsDist, err := cpuMasks.GetIRQsDistributionMasks(
			nvmeDisksInfo.Irqs, finalCpuMask)
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
	diskInfoByType map[disk.DiskType]disk.DevicesIRQs,
	cpuMasks irq.CpuMasks,
) (irq.Mode, error) {

	log.Debug("Calculating default mode for Disk IRQs")
	nonNvmeDiskIRQs := diskInfoByType[disk.NonNvme]
	if len(nonNvmeDiskIRQs.Devices) == 0 {
		return irq.Mq, nil
	}
	numOfCores, err := cpuMasks.GetNumberOfCores(cpuMask)
	if err != nil {
		return irq.Default, err
	}
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
