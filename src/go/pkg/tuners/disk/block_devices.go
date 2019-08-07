package disk

import (
	"path"
	"strings"
	"syscall"
	"vectorized/pkg/os"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type BlockDevices interface {
	GetDirectoriesDevices(directories []string) (map[string][]string, error)
	GetDirectoryDevices(directory string) ([]string, error)
	GetDeviceFromPath(path string) (BlockDevice, error)
	GetDeviceSystemPath(devicePath string) (string, error)
}

type blockDevices struct {
	proc os.Proc
	fs   afero.Fs
}

func NewBlockDevices(fs afero.Fs, proc os.Proc) BlockDevices {
	return &blockDevices{
		fs:   fs,
		proc: proc,
	}
}

func (b *blockDevices) GetDirectoriesDevices(
	directories []string,
) (map[string][]string, error) {
	dirDevices := make(map[string][]string)
	for _, directory := range directories {
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
	if !utils.FileExists(b.fs, path) {
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
	outputLines, err := b.proc.RunWithSystemLdPath("df", "-P", path)
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
