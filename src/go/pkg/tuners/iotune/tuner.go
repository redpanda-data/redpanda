package iotune

import (
	"time"
	"vectorized/pkg/os"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func NewIoTuneTuner(
	fs afero.Fs,
	evalDirectories []string,
	ioConfigFile string,
	duration int,
	timeout time.Duration,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		redpanda.NewIOConfigFileExistanceChecker(fs, ioConfigFile),
		func() tuners.TuneResult {
			return tune(evalDirectories, ioConfigFile, duration, timeout)
		},
		func() (bool, string) {
			return checkIfIoTuneIsSupported(fs)
		},
		false)
}

func checkIfIoTuneIsSupported(fs afero.Fs) (bool, string) {
	if exists, _ := afero.Exists(fs, "iotune"); !exists {
		return false, "Seastar iotune not found in PATH"
	}
	return true, ""
}

func tune(
	evalDirectories []string,
	ioConfigFile string,
	duration int,
	timeout time.Duration,
) tuners.TuneResult {
	ioTune := NewIoTune(os.NewProc(), timeout)
	args := IoTuneArgs{
		Dirs:           evalDirectories,
		Format:         Seastar,
		PropertiesFile: ioConfigFile,
		Duration:       duration,
		FsCheck:        false,
	}
	output, err := ioTune.Run(args)
	for _, outLine := range output {
		log.Debug(outLine)
	}
	if err != nil {
		return tuners.NewTuneError(err)
	}
	return tuners.NewTuneResult(false)
}
