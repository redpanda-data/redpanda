// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"bytes"
	"testing"
	"vectorized/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func getConfig() *config.Config {
	conf := config.Default()
	conf.Rpk.EnableUsageStats = true
	conf.ConfigFile = "/etc/redpanda/redpanda.yaml"
	return conf
}

func writeConfig(fs afero.Fs, conf *config.Config) error {
	bs, err := yaml.Marshal(conf)
	if err != nil {
		return err
	}
	return afero.WriteFile(fs, conf.ConfigFile, bs, 0644)
}

func TestStatus(t *testing.T) {
	defaultSetup := func(fs afero.Fs) error {
		return writeConfig(fs, getConfig())
	}
	tests := []struct {
		name           string
		expectedErr    string
		expectedOut    string
		expectNoReport bool
		args           []string
		before         func(afero.Fs) error
	}{
		{
			name:        "it should contain a version row",
			expectedOut: `\s\sVersion`,
			before:      defaultSetup,
		},
		{
			name:        "it should contain an OS info row",
			expectedOut: `\n\s\sOS[\s]+`,
			before:      defaultSetup,
		},
		{
			name:        "it should contain a CPU model row",
			expectedOut: `\n\s\sCPU\sModel[\s]+`,
			before:      defaultSetup,
		},
		{
			name:        "doesn't print the CPU% if no pid file is found",
			expectedOut: "open /var/lib/redpanda/data/pid.lock: file does not exist",
			before:      defaultSetup,
		},
		{
			name:        "fails if the pid file is empty",
			expectedOut: "/var/lib/redpanda/data/pid.lock is empty",
			before: func(fs afero.Fs) error {
				conf := getConfig()
				err := writeConfig(fs, conf)
				if err != nil {
					return err
				}
				_, err = fs.Create(conf.PIDFile())
				return err
			},
		},
		{
			name:        "fails if the pid file contains more than one line",
			expectedOut: "/var/lib/redpanda/data/pid.lock contains multiple lines",
			before: func(fs afero.Fs) error {
				conf := getConfig()
				err := writeConfig(fs, conf)
				if err != nil {
					return err
				}
				file, err := fs.Create(conf.PIDFile())
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
				conf := getConfig()
				err := writeConfig(fs, conf)
				if err != nil {
					return err
				}
				file, err := fs.Create(conf.PIDFile())
				if err != nil {
					return err
				}
				_, err = file.Write([]byte("Nope"))
				return err
			},
		},
		{
			name: "prints warning if enable_telemetry is set to false",
			expectedOut: "Usage stats reporting is disabled, so" +
				" nothing will be sent. To enable it, run" +
				" `rpk config set rpk.enable_usage_stats true`.",
			expectedErr: "open /var/lib/redpanda/data/pid.lock: file does not exist",
			args:        []string{"--send"},
			before: func(fs afero.Fs) error {
				conf := getConfig()
				conf.Rpk.EnableUsageStats = false
				return writeConfig(fs, conf)
			},
		},
	}
	logrus.SetLevel(logrus.DebugLevel)
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			mgr := config.NewManager(fs)
			if tt.before != nil {
				err := tt.before(fs)
				require.NoError(t, err)
			}
			var out bytes.Buffer
			cmd := NewStatusCommand(fs, mgr)
			cmd.SetArgs(tt.args)
			logrus.SetOutput(&out)
			err := cmd.Execute()
			if tt.expectedErr != "" {
				require.Error(t, err)
				require.Regexp(t, tt.expectedErr, err.Error())
				return
			}
			require.NoError(t, err)
			if tt.expectedOut != "" {
				require.Regexp(t, tt.expectedOut, out.String())
			}
			if tt.expectNoReport {
				require.NotRegexp(t, `\s\sVersion`, out.String())
				require.NotRegexp(t, `\s\sOS`, out.String())
				require.NotRegexp(t, `\s\sCPU Model`, out.String())
			}
		})
	}
}

func TestCompress(t *testing.T) {
	tests := []struct {
		name     string
		ints     []int
		expected []string
	}{
		{
			name:     "test 1",
			ints:     []int{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expected: []string{"1-20"},
		},
		{
			name:     "test 2",
			ints:     []int{0, 2, 3, 4, 5, 7, 9, 10, 12, 13, 14, 15, 16, 17, 18, 19, 20},
			expected: []string{"0", "2-5", "7", "9", "10", "12-20"},
		},
		{
			name:     "test 3",
			ints:     []int{0, 2, 4, 6, 8, 10, 12, 14, 16, 18, 20},
			expected: []string{"0", "2", "4", "6", "8", "10", "12", "14", "16", "18", "20"},
		},
		{
			name:     "test 4",
			ints:     []int{},
			expected: []string{},
		},
		{
			name:     "test 4",
			ints:     []int{1},
			expected: []string{"1"},
		},
		{
			name:     "test 4",
			ints:     []int{1, 2},
			expected: []string{"1", "2"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			require.Equal(st, tt.expected, compress(tt.ints))
		})
	}
}
