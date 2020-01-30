package cmd_test

import (
	"bytes"
	"strings"
	"testing"
	"vectorized/pkg/cli/cmd"
	"vectorized/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

const (
	pidFile    = "/var/lib/redpanda/pid"
	configPath = "/etc/redpanda/redpanda.yaml"
)

func getConfig() config.Config {
	return config.Config{
		PidFile: pidFile,
		Redpanda: &config.RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: config.SocketAddress{"127.0.0.1", 33145},
			Id:        1,
			KafkaApi:  config.SocketAddress{"127.0.0.1", 33145},
			SeedServers: []*config.SeedServer{
				&config.SeedServer{
					Host: config.SocketAddress{"127.0.0.1", 33145},
					Id:   1,
				},
				&config.SeedServer{
					Host: config.SocketAddress{"127.0.0.1", 33145},
					Id:   2,
				},
			},
		},
		Rpk: &config.RpkConfig{
			EnableUsageStats: true,
		},
	}
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
			name:        "fails if no config file is found",
			expectedErr: "Unable to find redpanda config file in default locations",
		},
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
				if err != nil {
					t.Errorf("got an error while setting up the test: %v", err)
				}
			}
			var out bytes.Buffer
			cmd := cmd.NewStatusCommand(fs)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if err != nil {
				if tt.expectedErr == "" {
					t.Fatalf("got an unexpected error: %v", err)
				}
				if !strings.Contains(err.Error(), tt.expectedErr) {
					t.Fatalf(
						"expected error message:\n%v\nto contain:\n%v",
						err.Error(),
						tt.expectedErr,
					)
				}
				return
			}
			if tt.expectedErr != "" {
				t.Fatal("expected an error, but got nil")
			}

			if tt.expectedOut != "" && !strings.Contains(out.String(), tt.expectedOut) {
				t.Fatalf(
					"expected output:\n%v\nto contain:\n%v",
					out.String(),
					tt.expectedOut,
				)
			}
		})
	}
}
