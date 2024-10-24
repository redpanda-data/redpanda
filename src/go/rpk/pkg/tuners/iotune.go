// Copyright 2020 Redpanda Data, Inc.
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
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

type ioTuner struct {
	duration        time.Duration
	evalDirectories []string
	fs              afero.Fs
	ioConfigFile    string
	timeout         time.Duration
}

func NewIoTuneTuner(
	fs afero.Fs,
	evalDirectories []string,
	ioConfigFile string,
	duration, timeout time.Duration,
) Tunable {
	return &ioTuner{
		duration:        duration,
		evalDirectories: evalDirectories,
		fs:              fs,
		ioConfigFile:    ioConfigFile,
		timeout:         timeout,
	}
}

func (tuner *ioTuner) CheckIfSupported() (bool, string) {
	if exists, _ := afero.Exists(tuner.fs, iotune.Bin); !exists {
		return false, fmt.Sprintf("'%s' not found in PATH", iotune.Bin)
	}
	return true, ""
}

func (tuner *ioTuner) Tune() TuneResult {
	ioTune := iotune.NewIoTune(os.NewProc(), tuner.timeout)
	args := iotune.IoTuneArgs{
		Dirs:           tuner.evalDirectories,
		Format:         iotune.Seastar,
		PropertiesFile: tuner.ioConfigFile,
		Duration:       tuner.duration,
		FsCheck:        false,
	}
	output, err := ioTune.Run(args)
	for _, outLine := range output {
		zap.L().Sugar().Debug(outLine)
	}
	if err != nil {
		return NewTuneError(err)
	}
	return NewTuneResult(false)
}
