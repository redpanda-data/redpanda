package irq

import (
	"path"
	"strings"
	"vectorized/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type DeviceInfo interface {
	GetIRQs(irqConfigDir string, xenDeviceName string) ([]string, error)
}

func NewDeviceInfo(fs afero.Fs, procFile ProcFile) DeviceInfo {

	return &deviceInfo{
		procFile: procFile,
		fs:       fs}
}

type deviceInfo struct {
	DeviceInfo
	procFile ProcFile
	fs       afero.Fs
}

func (deviceInfo *deviceInfo) GetIRQs(
	irqConfigDir string, xenDeviceName string,
) ([]string, error) {
	log.Debugf("Reading IRQs of '%s', with deviceInfo name pattern '%s'", irqConfigDir, xenDeviceName)
	msiIRQsDirName := path.Join(irqConfigDir, "msi_irqs")
	var irqs []string
	if utils.FileExists(deviceInfo.fs, msiIRQsDirName) {
		log.Debugf("Device '%s' uses MSI IRQs", irqConfigDir)
		irqs = utils.ListFilesInPath(deviceInfo.fs, msiIRQsDirName)
	} else {
		irqFileName := path.Join(irqConfigDir, "irq")
		if utils.FileExists(deviceInfo.fs, irqFileName) {
			log.Debugf("Device '%s' uses INT#x IRQs", irqConfigDir)
			lines, err := utils.ReadFileLines(deviceInfo.fs, irqFileName)
			if err != nil {
				return nil, err
			}
			for _, rawLine := range lines {
				irqs = append(irqs, strings.TrimSpace(rawLine))
			}
		} else {
			modAliasFileName := path.Join(irqConfigDir, "modalias")
			lines, err := utils.ReadFileLines(deviceInfo.fs, modAliasFileName)
			if err != nil {
				return nil, err
			}
			modAlias := lines[0]
			irqProcFileLines, err := deviceInfo.procFile.GetIRQProcFileLinesMap()
			if err != nil {
				return nil, err
			}
			if strings.Contains(modAlias, "virtio") {
				log.Debugf("Device '%s' is a virtio device type", irqConfigDir)
				fileNames := utils.ListFilesInPath(deviceInfo.fs, path.Join(irqConfigDir, "driver"))
				for _, name := range fileNames {
					if strings.Contains(name, "virtio") {
						irqs = append(irqs,
							deviceInfo.getIRQsForLinesMatching(name, irqProcFileLines)...)
					}
				}
			} else {
				if strings.Contains(modAlias, "xen:") {
					log.Debugf("Reading '%s' device IRQs from /proc/interrupts", irqConfigDir)
					irqs = deviceInfo.getIRQsForLinesMatching(xenDeviceName, irqProcFileLines)
				}
			}
		}
	}
	log.Debugf("DeviceInfo '%s' IRQs '%s'", irqConfigDir, irqs)
	return irqs, nil
}

func (deviceInfo *deviceInfo) getIRQsForLinesMatching(
	pattern string, irqToProcLineMap map[string]string,
) []string {
	var irqs []string
	for irq, line := range irqToProcLineMap {
		if strings.Contains(line, pattern) {
			irqs = append(irqs, irq)
		}
	}
	return irqs
}
