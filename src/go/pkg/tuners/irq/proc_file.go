package irq

import (
	"strings"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

type ProcFile interface {
	GetIRQProcFileLinesMap() (map[string]string, error)
}

func NewProcFile(fs afero.Fs) ProcFile {
	return &procFile{
		fs: fs,
	}
}

type procFile struct {
	fs afero.Fs
}

func (procFile *procFile) GetIRQProcFileLinesMap() (map[string]string, error) {
	log.Debugf("Reading '/proc/interrupts' file...")
	lines, err := utils.ReadFileLines(procFile.fs, "/proc/interrupts")
	if err != nil {
		return nil, err
	}
	linesByIRQ := make(map[string]string)
	for _, line := range lines {
		irq := strings.TrimSpace(strings.Split(line, ":")[0])
		linesByIRQ[irq] = line
	}
	for irq, line := range linesByIRQ {
		log.Tracef("IRQ -> /proc/interrupts %s - %s", irq, line)
	}
	return linesByIRQ, nil
}
