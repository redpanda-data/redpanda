package coredump

import (
	"bytes"
	"text/template"
	"vectorized/pkg/config"
	"vectorized/pkg/tuners"
	"vectorized/pkg/tuners/executors"
	"vectorized/pkg/tuners/executors/commands"

	"github.com/spf13/afero"
)

const (
	corePatternFilePath string = "/proc/sys/kernel/core_pattern"
	scriptFilePath      string = "/var/lib/redpanda/save_coredump"
	coredumpPattern     string = "| " + scriptFilePath + " %e %t %p"
	coredumpScriptTmpl  string = `#!/bin/bash
set -o errexit
set -o nounset
set -o pipefail
CMD=${1}
PID=${3}
TIMESTAMP_UTC=$(date --utc +'%Y-%m-%d_%H:%M:%S.%N_%Z')
COREDUMP_PATH={{.CoredumpDir}}/core."${CMD}"-"${TIMESTAMP_UTC}"-"${PID}"

mkdir -p {{.CoredumpDir}}
logger -p user.err "Saving ${CMD} coredump to ${COREDUMP_PATH}"
cat - > "${COREDUMP_PATH}"
`
)

type tuner struct {
	fs       afero.Fs
	conf     config.Config
	executor executors.Executor
}

func NewCoredumpTuner(
	fs afero.Fs, conf config.Config, executor executors.Executor,
) tuners.Tunable {
	return &tuner{fs, conf, executor}
}

func (t *tuner) Tune() tuners.TuneResult {
	script, err := renderTemplate(coredumpScriptTmpl, *t.conf.Rpk)
	if err != nil {
		return tuners.NewTuneError(err)
	}
	err = t.executor.Execute(commands.NewWriteFileModeCmd(t.fs, scriptFilePath, script, 0777))
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

func renderTemplate(
	templateStr string, conf config.RpkConfig,
) (string, error) {
	tmpl, err := template.New("template").Parse(templateStr)
	if err != nil {
		return "", err
	}
	var buf bytes.Buffer
	if err = tmpl.Execute(&buf, conf); err != nil {
		return "", err
	}
	return buf.String(), nil
}
