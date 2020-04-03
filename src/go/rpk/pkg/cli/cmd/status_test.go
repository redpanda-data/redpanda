package cmd_test

import (
	"bytes"
	"testing"
	"vectorized/pkg/cli/cmd"
	"vectorized/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

const (
	pidFile    = "/var/lib/redpanda/pid"
	configPath = "/etc/redpanda/redpanda.yaml"
)

func getConfig() config.Config {
	conf := config.DefaultConfig()
	conf.Rpk.EnableUsageStats = true
	return conf
}

func writeConfig(fs afero.Fs, conf config.Config) error {
	bs, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}
	return afero.WriteFile(fs, configPath, bs, 0644)
}

func TestStatus(t *testing.T) {
	tests := []struct {
		name        string
		expectedErr string
		expectedOut string
		args        []string
		before      func(afero.Fs) error
	}{
		{
			name:        "doesn't print the CPU% if no pid file is found",
			expectedOut: "No PID present",
			before: func(fs afero.Fs) error {
				err := writeConfig(fs, getConfig())
				if err != nil {
					return err
				}
				_, err = fs.Create(pidFile)
				return err
			},
		},
		{
			name:        "fails if the pid file is empty",
			expectedOut: "No PID present",
			before: func(fs afero.Fs) error {
				err := writeConfig(fs, getConfig())
				if err != nil {
					return err
				}
				_, err = fs.Create(pidFile)
				return err
			},
		},
		{
			name:        "fails if the pid file contains more than one line",
			expectedOut: "PID file corrupt",
			before: func(fs afero.Fs) error {
				err := writeConfig(fs, getConfig())
				if err != nil {
					return err
				}
				file, err := fs.Create(pidFile)
				if err != nil {
					return err
				}
				_, err = file.Write([]byte("1231\n4321"))
				return err
			},
		},
		{
			name:        "fails if pid file contents can't be parsed",
			expectedOut: "invalid syntax",
			before: func(fs afero.Fs) error {
				err := writeConfig(fs, getConfig())
				if err != nil {
					return err
				}
				file, err := fs.Create(pidFile)
				if err != nil {
					return err
				}
				_, err = file.Write([]byte("Nope"))
				return err
			},
		},
		{
			name:        "does nothing if enable_telemetry is set to false",
			expectedOut: "Usage stats are disabled. To enable them, set rpk.enable_usage_stats to true.",
			before: func(fs afero.Fs) error {
				conf := getConfig()
				conf.Rpk.EnableUsageStats = false
				return writeConfig(fs, conf)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(t, err)
			}
			var out bytes.Buffer
			cmd := cmd.NewStatusCommand(fs)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.EqualError(t, err, tt.expectedErr)
				return
			}
			require.NoError(t, err)
			if tt.expectedOut != "" {
				require.Contains(t, out.String(), tt.expectedOut)
			}
		})
	}
}
