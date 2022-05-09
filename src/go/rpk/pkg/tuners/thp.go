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
	"os"
	"path/filepath"
	"strings"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/system"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

const enabledFile = "enabled"

type thpTuner struct {
	fs       afero.Fs
	executor executors.Executor
}

// Returns the known locations where config files for Transparent Huge Pages
// might be found across distros.
func locations() []string {
	return []string{
		"/sys/kernel/mm/transparent_hugepage",        // default
		"/sys/kernel/mm/redhat_transparent_hugepage", // some versions of RHEL
	}
}

func getTHPDir(fs afero.Fs) (string, error) {
	for _, loc := range locations() {
		exists, err := afero.DirExists(fs, loc)
		if err != nil && !os.IsNotExist(err) {
			return "", err
		}
		if exists {
			return loc, nil
		}
	}
	return "", fmt.Errorf(
		"None of %s was found",
		strings.Join(locations(), ", "),
	)
}

// Create a new tuner to enable Transparent Huge Pages.
func NewEnableTHPTuner(fs afero.Fs, executor executors.Executor) Tunable {
	return &thpTuner{fs: fs, executor: executor}
}

func (t *thpTuner) CheckIfSupported() (bool, string) {
	dir, err := getTHPDir(t.fs)
	if err != nil {
		return false, err.Error()
	}
	return dir != "", ""
}

func (t *thpTuner) Tune() TuneResult {
	dir, err := getTHPDir(t.fs)
	if err != nil {
		return NewTuneError(err)
	}
	// Write 'always' to the 'enabled' file in the existing THP dir so that
	// they're always enabled.
	// https://www.kernel.org/doc/Documentation/vm/transhuge.txt

	cmd := commands.NewWriteFileCmd(
		t.fs,
		filepath.Join(dir, enabledFile),
		"always",
	)
	err = t.executor.Execute(cmd)
	if err != nil {
		return NewTuneError(err)
	}
	return NewTuneResult(false)
}

func NewTransparentHugePagesChecker(fs afero.Fs) Checker {
	return NewEqualityChecker(
		TransparentHugePagesChecker,
		"Transparent huge pages active",
		Warning,
		true,
		func() (interface{}, error) {
			return system.GetTransparentHugePagesActive(fs)
		},
	)
}
