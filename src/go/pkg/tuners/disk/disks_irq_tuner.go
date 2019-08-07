package disk

import (
	"path"
	"regexp"
	"strconv"
	"strings"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/irq"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
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
	irqDeviceInfo       irq.DeviceInfo
	cpuMasks            irq.CpuMasks
	irqBalanceService   irq.BalanceService
	irqProcFile         irq.ProcFile
	diskInfoProvider    InfoProvider
	mode                irq.Mode
	baseCPUMask         string
	computationsCPUMask string
	irqCPUMask          string
	directories         []string
	devices             []string
	directoryDevice     map[string][]string
	diskInfoByType      map[diskType]devicesIRQs
	deviceIRQs          map[string][]int
	numberOfCpus        int
}

func NewDiskIrqTuner(
	mode irq.Mode,
	cpuMask string,
	dirs []string,
	devices []string,
	irqDeviceInfo irq.DeviceInfo,
	cpuMasks irq.CpuMasks,
	irqBalanceService irq.BalanceService,
	irqProcFile irq.ProcFile,
	diskInfoProvider InfoProvider,
	numberOfCpus int,
) tuners.Tunable {
	log.Debugf("Creating disk IRQs tuner with mode '%s', cpu mask '%s', directories '%s' and devices '%s'",
		mode, cpuMask, dirs, devices)

	return &disksIRQsTuner{
		irqDeviceInfo:     irqDeviceInfo,
		cpuMasks:          cpuMasks,
		irqBalanceService: irqBalanceService,
		irqProcFile:       irqProcFile,
		diskInfoProvider:  diskInfoProvider,
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

func (tuner *disksIRQsTuner) getDefaultMode() (irq.Mode, error) {
	if tuner.mode != irq.Default {
		return tuner.mode, nil
	}
	log.Debugf("Calculating default mode for Disk IrqTuner")
	nonNvmeDiskIrqs := tuner.diskInfoByType[nonNvme]
	if len(nonNvmeDiskIrqs.devices) == 0 {
		return irq.Mq, nil
	}
	numOfCores, err := tuner.cpuMasks.GetNumberOfCores(tuner.baseCPUMask)
	numOfPUs, err := tuner.cpuMasks.GetNumberOfPUs(tuner.baseCPUMask)
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

func (tuner *disksIRQsTuner) Tune() tuners.TuneResult {
	var err error
	tuner.baseCPUMask, err = tuner.cpuMasks.BaseCpuMask(tuner.baseCPUMask)
	tuner.mode, err = tuner.getDefaultMode()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.computationsCPUMask, err = tuner.cpuMasks.CpuMaskForComputations(
		tuner.mode, tuner.baseCPUMask)
	tuner.irqCPUMask, err = tuner.cpuMasks.CpuMaskForIRQs(
		tuner.mode, tuner.baseCPUMask)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.directoryDevice, err = tuner.diskInfoProvider.GetDirectoriesDevices(
		tuner.directories)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.deviceIRQs, err = tuner.getDevicesIRQs()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	tuner.diskInfoByType, err = tuner.groupDiskInfoByType()
	if err != nil {
		return tuners.NewTuneError(err)
	}
	if err = tuner.irqBalanceService.BanIRQsAndRestart(tuner.getAllIRQs()); err != nil {
		return tuners.NewTuneError(err)
	}
	nonNvmeDisksInfo := tuner.diskInfoByType[nonNvme]
	nvmeDisksInfo := tuner.diskInfoByType[nvme]

	if len(nonNvmeDisksInfo.devices) > 0 {
		log.Infof("Tuning non-Nvme disks %s", nonNvmeDisksInfo.devices)
		if err := tuner.cpuMasks.DistributeIRQs(
			nonNvmeDisksInfo.irqs, tuner.irqCPUMask); err != nil {
			return tuners.NewTuneError(err)
		}
	} else {
		log.Infof("No non-Nvme disks to tune")
	}

	if len(nvmeDisksInfo.devices) > 0 {
		log.Infof("Tuning Nvme disks %s", nvmeDisksInfo.devices)
		if err := tuner.cpuMasks.DistributeIRQs(nvmeDisksInfo.irqs,
			tuner.baseCPUMask); err != nil {
			return tuners.NewTuneError(err)
		}
	} else {
		log.Infof("No Nvme disks to tune")
	}
	return tuners.NewTuneResult(false)
}

func (tuner *disksIRQsTuner) getAllIRQs() []int {
	irqsSet := map[int]bool{}
	for _, irqs := range tuner.deviceIRQs {
		for _, irq := range irqs {
			irqsSet[irq] = true
		}
	}
	return utils.GetIntKeys(irqsSet)
}

func (tuner *disksIRQsTuner) getDevicesIRQs() (map[string][]int, error) {
	diskIRQs := make(map[string][]int)

	for _, devices := range tuner.directoryDevice {
		for _, device := range devices {
			if diskIRQs[device] != nil {
				continue
			}
			controllerPath, err := tuner.getDeviceControllerPath(device)
			if err != nil {
				return nil, err
			}
			interrupts, err := tuner.irqDeviceInfo.GetIRQs(controllerPath,
				"blkif")
			if err != nil {
				return nil, err
			}
			diskIRQs[device] = interrupts
		}
	}
	for _, device := range tuner.devices {
		if diskIRQs[device] != nil {
			continue
		}
		controllerPath, err := tuner.getDeviceControllerPath(device)
		if err != nil {
			return nil, err
		}
		interrupts, err := tuner.irqDeviceInfo.GetIRQs(controllerPath,
			"blkif")
		if err != nil {
			return nil, err
		}
		diskIRQs[device] = interrupts
	}
	return diskIRQs, nil
}

func (tuner *disksIRQsTuner) getDeviceControllerPath(
	device string,
) (string, error) {
	devicePath := path.Join("/dev", device)
	log.Debugf("Getting controller path for '%s'", devicePath)
	devSystemPath, err := tuner.diskInfoProvider.GetBlockDeviceSystemPath(
		devicePath)
	if err != nil {
		return "", err
	}
	splitSystemPath := strings.Split(devSystemPath, "/")
	controllerPathParts := append([]string{"/"}, splitSystemPath[0:4]...)
	pattern, _ := regexp.Compile(
		"^[0-9ABCDEFabcdef]{4}:[0-9ABCDEFabcdef]{2}:[0-9ABCDEFabcdef]{2}\\.[0-9ABCDEFabcdef]$")
	for _, systemPathPart := range splitSystemPath[4:] {
		if pattern.MatchString(systemPathPart) {
			controllerPathParts = append(controllerPathParts, systemPathPart)
		} else {
			break
		}
	}
	return path.Join(controllerPathParts...), nil
}

func (tuner *disksIRQsTuner) groupDiskInfoByType() (
	map[diskType]devicesIRQs,
	error,
) {
	diskInfoByType := make(map[diskType]devicesIRQs)
	// using map in order to provide set functionality
	nvmeDisks := map[string]bool{}
	nvmeIRQs := map[int]bool{}
	nonNvmeDisks := map[string]bool{}
	nonNvmeIRQs := map[int]bool{}

	for device, irqs := range tuner.deviceIRQs {
		if strings.HasPrefix(device, "nvme") {
			nvmeDisks[device] = true
			for _, singleIrq := range irqs {
				nvmeIRQs[singleIrq] = true
			}
		} else {
			nonNvmeDisks[device] = true
			for _, singleIrq := range irqs {
				nonNvmeIRQs[singleIrq] = true
			}
		}
	}

	if utils.IsAWSi3MetalInstance() {
		for singleIrq := range nvmeIRQs {
			isNvmFastPath, err := tuner.isIRQNvmeFastPathIrq(singleIrq)
			if err != nil {
				return nil, err
			}
			if !isNvmFastPath {
				delete(nvmeIRQs, singleIrq)
			}
		}
	}
	diskInfoByType[nvme] = devicesIRQs{utils.GetKeys(nvmeDisks),
		utils.GetIntKeys(nvmeIRQs)}
	diskInfoByType[nonNvme] = devicesIRQs{utils.GetKeys(nonNvmeDisks),
		utils.GetIntKeys(nonNvmeIRQs)}
	return diskInfoByType, nil
}

func (tuner *disksIRQsTuner) isIRQNvmeFastPathIrq(irq int) (bool, error) {
	nvmeFastPathQueuePattern := regexp.MustCompile(
		"(\\s|^)nvme\\d+q(\\d+)(\\s|$)")
	linesMap, err := tuner.irqProcFile.GetIRQProcFileLinesMap()
	if err != nil {
		return false, err
	}
	splitProcLine := strings.Split(linesMap[irq], ",")
	for _, part := range splitProcLine {
		matches := nvmeFastPathQueuePattern.FindAllStringSubmatch(part, -1)
		if matches != nil {
			queueNumber, _ := strconv.ParseInt(matches[0][2], 10, 8)
			if int(queueNumber) <= tuner.numberOfCpus {
				return true, nil
			}
		}
	}
	return false, nil
}
