// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package ballast

import (
	"fmt"
	"path/filepath"

	"github.com/docker/go-units"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
)

type ballastTuner struct {
	conf     config.Config
	executor executors.Executor
}

func NewBallastFileTuner(
	conf config.Config, executor executors.Executor,
) tuners.Tunable {
	return &ballastTuner{conf, executor}
}

func (t *ballastTuner) Tune() tuners.TuneResult {
	path := config.DefaultBallastFilePath
	if t.conf.Rpk.Tuners.BallastFilePath != "" {
		path = t.conf.Rpk.Tuners.BallastFilePath
	}
	abspath, err := filepath.Abs(path)
	if err != nil {
		return tuners.NewTuneError(fmt.Errorf(
			"couldn't resolve the absolute file path for %s: %w",
			path,
			err,
		))
	}

	size := config.DefaultBallastFileSize
	if t.conf.Rpk.Tuners.BallastFileSize != "" {
		size = t.conf.Rpk.Tuners.BallastFileSize
	}
	sizeBytes, err := units.FromHumanSize(size)
	if err != nil {
		return tuners.NewTuneError(fmt.Errorf(
			"'%s' is not a valid size unit",
			size,
		))
	}

	cmd := commands.NewWriteSizedFileCmd(abspath, sizeBytes)
	err = t.executor.Execute(cmd)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	return tuners.NewTuneResult(false)
}

func (*ballastTuner) CheckIfSupported() (supported bool, reason string) {
	return true, ""
}
