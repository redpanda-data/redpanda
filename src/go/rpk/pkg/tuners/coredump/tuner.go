// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package coredump

import (
	"bytes"
	"text/template"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/executors/commands"
	"github.com/spf13/afero"
)

const (
	corePatternFilePath string = "/proc/sys/kernel/core_pattern"
	scriptFilePath      string = "/var/lib/redpanda/save_coredump"
	coredumpPattern     string = "|" + scriptFilePath + " %e %t %p"
	coredumpScriptTmpl  string = `#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
CMD=${1}
PID=${3}
TIMESTAMP_UTC=$(date --utc +'%Y-%m-%d_%H:%M:%S.%N_%Z')
COREDUMP_PATH={{.}}/core."${CMD}"-"${TIMESTAMP_UTC}"-"${PID}"

mkdir -p {{.}}
logger -p user.err "Saving ${CMD} coredump to ${COREDUMP_PATH}"
cat - > "${COREDUMP_PATH}"
` // input variable is the CoredumpDir
)

type tuner struct {
	fs          afero.Fs
	coredumpDir string
	executor    executors.Executor
}

func NewCoredumpTuner(
	fs afero.Fs, coredumpDir string, executor executors.Executor,
) tuners.Tunable {
	return &tuner{fs, coredumpDir, executor}
}

func (t *tuner) Tune() tuners.TuneResult {
	script, err := renderTemplate(coredumpScriptTmpl, t.coredumpDir)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	err = t.executor.Execute(commands.NewWriteFileModeCmd(t.fs, scriptFilePath, script, 0o777))
	if err != nil {
		return tuners.NewTuneError(err)
	}
	err = t.executor.Execute(commands.NewWriteFileCmd(t.fs, corePatternFilePath, coredumpPattern))
	if err != nil {
		return tuners.NewTuneError(err)
	}
	return tuners.NewTuneResult(false)
}

func (*tuner) CheckIfSupported() (supported bool, reason string) {
	return true, ""
}

func renderTemplate(templateStr string, input string) (string, error) {
	tmpl, err := template.New("template").Parse(templateStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, input); err != nil {
		return "", err
	}
	return buf.String(), nil
}
