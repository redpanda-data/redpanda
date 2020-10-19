package disk

import (
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"golang.org/x/sys/unix"
)

func NewDevice(dev uint64, fs afero.Fs) (BlockDevice, error) {
	maj := unix.Major(dev)
	min := unix.Minor(dev)
	log.Debugf("Creating block device from number {%d, %d}", maj, min)
	syspath, err := readSyspath(maj, min)
	if err != nil {
		return nil, err
	}
	return deviceFromSystemPath(syspath, fs)
}
