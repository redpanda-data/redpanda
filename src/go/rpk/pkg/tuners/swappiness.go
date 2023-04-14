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
	"strconv"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
	"go.uber.org/zap"
)

const (
	File               string = "/proc/sys/vm/swappiness"
	ExpectedSwappiness int    = 1
)

func NewSwappinessChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		Swappiness,
		"Swappiness",
		Warning,
		ExpectedSwappiness,
		func() (interface{}, error) {
			content, err := afero.ReadFile(fs, File)
			if err != nil {
				return -1, err
			}
			return strconv.Atoi(strings.TrimSpace(string(content)))
		},
	)
}

func NewSwappinessTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return NewCheckedTunable(
		NewSwappinessChecker(fs),
		func() TuneResult {
			zap.L().Sugar().Debugf("Setting swappiness to %d", ExpectedSwappiness)
			err := executor.Execute(
				commands.NewWriteFileCmd(
					fs, File, fmt.Sprint(ExpectedSwappiness)))
			if err != nil {
				zap.L().Sugar().Errorf("got an error while writing %d to %s: %v", ExpectedSwappiness, File, err)
				return NewTuneError(err)
			}
			return NewTuneResult(false)
		},
		func() (bool, string) {
			return true, ""
		},
		executor.IsLazy(),
	)
}
