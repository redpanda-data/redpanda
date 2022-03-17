// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package tuners

import (
	"fmt"
	"time"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/os"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/afero"
)

func NewIoTuneTuner(
	fs afero.Fs,
	evalDirectories []string,
	ioConfigFile string,
	duration, timeout time.Duration,
) Tunable {
	return NewCheckedTunable(
		NewIOConfigFileExistanceChecker(fs, ioConfigFile),
		func() TuneResult {
			return tune(evalDirectories, ioConfigFile, duration, timeout)
		},
		func() (bool, string) {
			return checkIfIoTuneIsSupported(fs)
		},
		false)
}

func checkIfIoTuneIsSupported(fs afero.Fs) (bool, string) {
	if exists, _ := afero.Exists(fs, iotune.Bin); !exists {
		return false, fmt.Sprintf("'%s' not found in PATH", iotune.Bin)
	}
	return true, ""
}

func tune(
	evalDirectories []string,
	ioConfigFile string,
	duration, timeout time.Duration,
) TuneResult {
	ioTune := iotune.NewIoTune(os.NewProc(), timeout)
	args := iotune.IoTuneArgs{
		Dirs:           evalDirectories,
		Format:         iotune.Seastar,
		PropertiesFile: ioConfigFile,
		Duration:       duration,
		FsCheck:        false,
	}
	output, err := ioTune.Run(args)
	for _, outLine := range output {
		log.Debug(outLine)
	}
	if err != nil {
		return NewTuneError(err)
	}
	return NewTuneResult(false)
}
