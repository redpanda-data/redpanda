package cmd_test

import (
	"bytes"
	"fmt"
	"os/exec"
	"strconv"
	"testing"
	"vectorized/pkg/cli/cmd"
	"vectorized/pkg/config"
	"vectorized/pkg/os"
	"vectorized/pkg/utils"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestStopCommand(t *testing.T) {
	const confPath string = "/etc/redpanda/redpanda.yaml"
	const pidFile string = "/tmp/redpanda-pid"
	// simulate the redpanda process with an infinite loop.
	const baseCommand string = "while :; do sleep 1; done"
	tests := []struct {
		name           string
		ignoredSignals []string
		args           []string
	}{
		{
			name: "it should stop redpanda on SIGINT",
			args: []string{"--timeout", "100ms"},
		},
		{
			name:           "it should stop redpanda on SIGTERM if SIGINT was ignored",
			ignoredSignals: []string{"INT"},
			args:           []string{"--timeout", "100ms"},
		},
		{
			name:           "it should stop redpanda on SIGKILL if SIGINT and SIGTERM were ignored",
			ignoredSignals: []string{"TERM", "INT"},
			args:           []string{"--timeout", "100ms"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			command := baseCommand
			// trap the signals we want to ignore, to check that the
			// signal escalation is working.
			for _, s := range tt.ignoredSignals {
				command = fmt.Sprintf(`trap "" %s; %s`, s, command)
			}
			ecmd := exec.Command("bash", "-c", command)
			// spawn the process asynchronously
			err := ecmd.Start()
			require.NoError(t, err)
			require.NotNil(t, ecmd.Process)

			pid := ecmd.Process.Pid
			_, err = utils.WriteBytes(
				fs,
				[]byte(strconv.Itoa(pid)),
				pidFile,
			)
			require.NoError(t, err)

			conf := config.DefaultConfig()
			conf.PidFile = pidFile
			err = config.WriteConfig(fs, &conf, confPath)
			require.NoError(t, err)

			var out bytes.Buffer
			c := cmd.NewStopCommand(fs)
			args := append([]string{"--config", confPath}, tt.args...)
			c.SetArgs(args)

			logrus.SetOutput(&out)
			err = c.Execute()
			require.NoError(t, err)

			isStillRunning, err := os.IsRunningPID(
				fs,
				ecmd.Process.Pid,
			)
			require.NoError(t, err)
			require.False(t, isStillRunning)
			_, err = utils.ReadEnsureSingleLine(fs, pidFile)
			// Check that rpk stop removed the PID file
			require.EqualError(t, err, "open /tmp/redpanda-pid: file does not exist")
		})
	}
}
