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

type InfoProvider interface {
	GetDirectoriesDevices(directories []string) (map[string][]string, error)
	GetBlockDeviceFromPath(path string) (BlockDevice, error)
	GetBlockDeviceSystemPath(devicePath string) (string, error)
}

type infoProvider struct {
	proc os.Proc
	fs   afero.Fs
}

func NewDiskInfoProvider(fs afero.Fs, proc os.Proc) InfoProvider {
	return &infoProvider{
		fs:   fs,
		proc: proc,
	}
}

func (infoProvider *infoProvider) GetDirectoriesDevices(
	directories []string,
) (map[string][]string, error) {
	dirDevices := make(map[string][]string)
	for _, directory := range directories {
		devices, err := infoProvider.getDirectoryDevices(directory)
		if err != nil {
			return nil, err
		}
		dirDevices[directory] = devices
	}
	return dirDevices, nil
}

func (infoProvider *infoProvider) getDirectoryDevices(
	path string,
) ([]string, error) {
	log.Debugf("Collecting info about directory '%s'", path)
	if !utils.FileExists(infoProvider.fs, path) {
		// path/to/whatever does not exist
		return []string{}, nil
	}
	device, err := infoProvider.getBlockDeviceFromPath(path, getDevNumFromDirectory)
	if err != nil {
		return nil, err
	}
	if device != nil {
		return infoProvider.getPhysDevices(device)
	}

	var devices []string
	outputLines, err := infoProvider.proc.RunWithSystemLdPath("df", "-P", path)
	if err != nil {
		return nil, err
	}
	for _, line := range outputLines[1:] {
		devicePath := strings.Split(line, " ")[0]
		if !strings.HasPrefix(devicePath, "/dev") {
			directoryDevices, err := infoProvider.getDirectoryDevices(devicePath)
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

func (infoProvider *infoProvider) getPhysDevices(
	device BlockDevice,
) ([]string, error) {
	log.Debugf("Getting physical device from '%s'", device.Syspath())
	if strings.Contains(device.Syspath(), "virtual") {
		joinedPath := path.Join(device.Syspath(), "slaves")
		files, err := afero.ReadDir(infoProvider.fs, joinedPath)
		if err != nil {
			return nil, err
		}
		var physDevices []string
		for _, deviceDirectory := range files {
			slavePath := "/dev/" + deviceDirectory.Name()
			log.Debugf("Dealing with virtual device, checking slave %s", slavePath)
			deviceFromPath, err := infoProvider.GetBlockDeviceFromPath(slavePath)
			if err != nil {
				return nil, err
			}
			devices, err := infoProvider.getPhysDevices(deviceFromPath)
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

func (infoProvider *infoProvider) GetBlockDeviceFromPath(
	path string,
) (BlockDevice, error) {
	return infoProvider.getBlockDeviceFromPath(path,
		getDevNumFromDeviceDirectory)
}

func (infoProvider *infoProvider) GetBlockDeviceSystemPath(
	path string,
) (string, error) {
	device, err := infoProvider.getBlockDeviceFromPath(path,
		getDevNumFromDeviceDirectory)
	return device.Syspath(), err
}

func (infoProvider *infoProvider) getBlockDeviceFromPath(
	path string, devNumExtractor func(syscall.Stat_t) uint64,
) (BlockDevice, error) {
	var stat syscall.Stat_t
	log.Debugf("Getting block device from path '%s'", path)
	err := syscall.Stat(path, &stat)
	if err != nil {
		return nil, err
	}
	number := devNumExtractor(stat)
	return NewDevice(int((0xFFFFFFFF00000000&number)>>32), int(number&0xFFFFFFFF), infoProvider.fs)
}
