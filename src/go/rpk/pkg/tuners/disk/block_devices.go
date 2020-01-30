package disk

import (
	"fmt"
	"path"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"time"
	"vectorized/pkg/os"
	"vectorized/pkg/tuners/irq"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type DiskType string

const (
	NonNvme DiskType = "non-nvme"
	Nvme    DiskType = "nvme"
)

type DevicesIRQs struct {
	Devices []string
	Irqs    []int
}
type BlockDevices interface {
	GetDirectoriesDevices(directories []string) (map[string][]string, error)
	GetDirectoryDevices(directory string) ([]string, error)
	GetDeviceFromPath(path string) (BlockDevice, error)
	GetDeviceSystemPath(devicePath string) (string, error)
	GetDiskInfoByType(devices []string) (map[DiskType]DevicesIRQs, error)
}

type blockDevices struct {
	proc          os.Proc
	fs            afero.Fs
	irqDeviceInfo irq.DeviceInfo
	irqProcFile   irq.ProcFile
	timeout       time.Duration
}

func NewBlockDevices(
	fs afero.Fs,
	irqDeviceInfo irq.DeviceInfo,
	irqProcFile irq.ProcFile,
	proc os.Proc,
	timeout time.Duration,
) BlockDevices {
	return &blockDevices{
		fs:            fs,
		proc:          proc,
		irqDeviceInfo: irqDeviceInfo,
		irqProcFile:   irqProcFile,
		timeout:       timeout,
	}
}

func (b *blockDevices) GetDirectoriesDevices(
	directories []string,
) (map[string][]string, error) {
	dirDevices := make(map[string][]string)
	for _, directory := range directories {
		if exists, _ := afero.Exists(b.fs, directory); !exists {
			return nil, fmt.Errorf("Directory '%s' does not exists", directory)
		}
		devices, err := b.GetDirectoryDevices(directory)
		if err != nil {
			return nil, err
		}
		dirDevices[directory] = devices
	}
	return dirDevices, nil
}

func (b *blockDevices) GetDirectoryDevices(path string) ([]string, error) {
	log.Debugf("Collecting info about directory '%s'", path)
	if exists, _ := afero.Exists(b.fs, path); !exists {
		// path/to/whatever does not exist
		return []string{}, nil
	}
	device, err := b.getBlockDeviceFromPath(path, getDevNumFromDirectory)
	if err != nil {
		return nil, err
	}
	if device != nil {
		return b.getPhysDevices(device)
	}

	var devices []string
	outputLines, err := b.proc.RunWithSystemLdPath(b.timeout, "df", "-P", path)
	if err != nil {
		return nil, err
	}
	for _, line := range outputLines[1:] {
		devicePath := strings.Split(line, " ")[0]
		if !strings.HasPrefix(devicePath, "/dev") {
			directoryDevices, err := b.GetDirectoryDevices(devicePath)
			if err != nil {
				return nil, err
			}
			devices = append(devices, directoryDevices...)
		} else {
			log.Error("Failed to create device"+
				" while 'df -P %s' returns a '%s'",
				path, devicePath)
		}
	}
	if len(devices) == 0 {
		log.Errorf("Can't get a block device for '%s' - skipping", path)
	}

	return []string{}, nil
}

func (b *blockDevices) getPhysDevices(device BlockDevice) ([]string, error) {
	log.Debugf("Getting physical device from '%s'", device.Syspath())
	if strings.Contains(device.Syspath(), "virtual") {
		joinedPath := path.Join(device.Syspath(), "slaves")
		files, err := afero.ReadDir(b.fs, joinedPath)
		if err != nil {
			return nil, err
		}
		var physDevices []string
		for _, deviceDirectory := range files {
			slavePath := "/dev/" + deviceDirectory.Name()
			log.Debugf("Dealing with virtual device, checking slave %s", slavePath)
			deviceFromPath, err := b.GetDeviceFromPath(slavePath)
			if err != nil {
				return nil, err
			}
			devices, err := b.getPhysDevices(deviceFromPath)
			if err != nil {
				return nil, err
			}
			physDevices = append(physDevices, devices...)
		}
		return physDevices, nil
	}
	return []string{strings.Replace(device.Devnode(),
		"/dev/", "", 1)}, nil
}

func getDevNumFromDeviceDirectory(stat syscall.Stat_t) uint64 {
	return stat.Rdev
}

func getDevNumFromDirectory(stat syscall.Stat_t) uint64 {
	return stat.Dev
}

func (b *blockDevices) GetDeviceFromPath(path string) (BlockDevice, error) {
	return b.getBlockDeviceFromPath(path,
		getDevNumFromDeviceDirectory)
}

func (b *blockDevices) GetDeviceSystemPath(path string) (string, error) {
	device, err := b.getBlockDeviceFromPath(path,
		getDevNumFromDeviceDirectory)
	return device.Syspath(), err
}

func (b *blockDevices) getBlockDeviceFromPath(
	path string, devNumExtractor func(syscall.Stat_t) uint64,
) (BlockDevice, error) {
	var stat syscall.Stat_t
	log.Debugf("Getting block device from path '%s'", path)
	err := syscall.Stat(path, &stat)
	if err != nil {
		return nil, err
	}
	number := devNumExtractor(stat)
	return NewDevice(int((0xFFFFFFFF00000000&number)>>32), int(number&0xFFFFFFFF), b.fs)
}

func (b *blockDevices) getDevicesIRQs(
	devices []string,
) (map[string][]int, error) {
	diskIRQs := make(map[string][]int)
	for _, device := range devices {
		if _, exists := diskIRQs[device]; exists {
			continue
		}
		log.Debugf("Getting '%s' IRQs", device)
		devicePath := path.Join("/dev", device)
		devSystemPath, err := b.GetDeviceSystemPath(devicePath)
		if err != nil {
			return nil, err
		}
		controllerPath, err := b.getDeviceControllerPath(devSystemPath)
		if err != nil {
			return nil, err
		}

		IRQs, err := b.irqDeviceInfo.GetIRQs(controllerPath, "blkif")
		if err != nil {
			return nil, err
		}
		diskIRQs[device] = IRQs
	}
	return diskIRQs, nil
}

func (b *blockDevices) getDeviceControllerPath(
	devSystemPath string,
) (string, error) {
	log.Debugf("Getting controller path for '%s'", devSystemPath)
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

func (b *blockDevices) GetDiskInfoByType(
	devices []string,
) (map[DiskType]DevicesIRQs, error) {
	diskInfoByType := make(map[DiskType]DevicesIRQs)
	// using map in order to provide set functionality
	nvmeDisks := map[string]bool{}
	nvmeIRQs := map[int]bool{}
	nonNvmeDisks := map[string]bool{}
	nonNvmeIRQs := map[int]bool{}
	deviceIRQs, err := b.getDevicesIRQs(devices)
	if err != nil {
		return nil, err
	}
	for device, irqs := range deviceIRQs {
		if strings.HasPrefix(device, "nvme") {
			nvmeDisks[device] = true
			for _, IRQ := range irqs {
				nvmeIRQs[IRQ] = true
			}
		} else {
			nonNvmeDisks[device] = true
			for _, IRQ := range irqs {
				nonNvmeIRQs[IRQ] = true
			}
		}
	}

	if utils.IsAWSi3MetalInstance() {
		for IRQ := range nvmeIRQs {
			isNvmFastPath, err := b.isIRQNvmeFastPathIRQ(IRQ, runtime.NumCPU())
			if err != nil {
				return nil, err
			}
			if !isNvmFastPath {
				delete(nvmeIRQs, IRQ)
			}
		}
	}
	diskInfoByType[Nvme] = DevicesIRQs{utils.GetKeys(nvmeDisks),
		utils.GetIntKeys(nvmeIRQs)}
	diskInfoByType[NonNvme] = DevicesIRQs{utils.GetKeys(nonNvmeDisks),
		utils.GetIntKeys(nonNvmeIRQs)}
	return diskInfoByType, nil
}

func (b *blockDevices) isIRQNvmeFastPathIRQ(
	irq, numberOfCpus int,
) (bool, error) {
	nvmeFastPathQueuePattern := regexp.MustCompile(
		"(\\s|^)nvme\\d+q(\\d+)(\\s|$)")
	linesMap, err := b.irqProcFile.GetIRQProcFileLinesMap()
	if err != nil {
		return false, err
	}
	splitProcLine := strings.Split(linesMap[irq], ",")
	for _, part := range splitProcLine {
		matches := nvmeFastPathQueuePattern.FindAllStringSubmatch(part, -1)
		if matches != nil {
			queueNumber, _ := strconv.ParseInt(matches[0][2], 10, 8)
			if int(queueNumber) <= numberOfCpus {
				return true, nil
			}
		}
	}
	return false, nil
}
