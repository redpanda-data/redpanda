package iotune

import (
	"vectorized/pkg/checkers"
	"vectorized/pkg/os"
	"vectorized/pkg/tuners"
	"vectorized/pkg/utils"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func NewIoTuneTuner(
	fs afero.Fs,
	evalDirectories []string,
	ioConfigFile string,
	duration int,
) tuners.Tunable {
	return tuners.NewCheckedTunable(
		checkers.NewIOConfigFileExistanceChecker(fs, ioConfigFile),
		func() tuners.TuneResult {
			return tune(evalDirectories, ioConfigFile, duration)
		},
		func() (bool, string) {
			return checkIfIoTuneIsSupported(fs)
		})
}

func checkIfIoTuneIsSupported(fs afero.Fs) (bool, string) {
	if !utils.FileExists(fs, "iotune") {
		return false, "Seastar iotune not found in PATH"
	}
	return true, ""
}

func tune(
	evalDirectories []string, ioConfigFile string, duration int,
) tuners.TuneResult {
	ioTune := NewIoTune(os.NewProc())
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
