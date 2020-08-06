package cmd

import (
	"bytes"
	"fmt"
	"os"
	"strings"
	"testing"
	"vectorized/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v2"
)

func fillRpkConfig(path, mode string) config.Config {
	conf := config.DefaultConfig()
	val := mode == config.ModeProd
	conf.Rpk = &config.RpkConfig{
		TuneNetwork:       val,
		TuneDiskScheduler: val,
		TuneNomerges:      val,
		TuneDiskIrq:       val,
		TuneFstrim:        val,
		TuneCpu:           val,
		TuneAioEvents:     val,
		TuneClocksource:   val,
		TuneSwappiness:    val,
		CoredumpDir:       path,
	}
	return conf
}

func TestModeCommand(t *testing.T) {
	configPath := "/etc/redpanda/redpanda.yaml"
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	wdConfigPath := fmt.Sprintf("%s/redpanda.yaml", dir)
	tests := []struct {
		name           string
		args           []string
		before         func(afero.Fs) (string, error)
		expectedConfig config.Config
		expectedOutput string
		expectedErrMsg string
	}{
		{
			name: "development mode should disable all fields in the rpk config",
			args: []string{"development", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkConfig(configPath, config.ModeProd))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: fillRpkConfig(configPath, config.ModeDev),
			expectedOutput: fmt.Sprintf("Writing 'development' mode defaults to '%s'", configPath),
			expectedErrMsg: "",
		},
		{
			name: "production mode should enable all fields in the rpk config",
			args: []string{"production", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkConfig(configPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: fillRpkConfig(configPath, config.ModeProd),
			expectedOutput: fmt.Sprintf("Writing 'production' mode defaults to '%s'", configPath),
			expectedErrMsg: "",
		},
		{
			name: "the development mode alias, 'dev', should work the same",
			args: []string{"dev", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkConfig(configPath, config.ModeProd))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: fillRpkConfig(configPath, config.ModeDev),
			expectedOutput: fmt.Sprintf("Writing 'dev' mode defaults to '%s'", configPath),
			expectedErrMsg: "",
		},
		{
			name: "the production mode alias, 'prod', should work the same",
			args: []string{"prod", "--config", configPath},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkConfig(configPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: fillRpkConfig(configPath, config.ModeProd),
			expectedOutput: fmt.Sprintf("Writing 'prod' mode defaults to '%s'", configPath),
			expectedErrMsg: "",
		},
		{
			name: "mode should work if --config isn't passed, but the file is in /etc/redpanda/redpanda.yaml",
			args: []string{"prod"},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkConfig(configPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: fillRpkConfig(configPath, config.ModeProd),
			expectedOutput: fmt.Sprintf("Writing 'prod' mode defaults to '%s'", configPath),
			expectedErrMsg: "",
		},
		{
			name: "mode lists the available modes if the one passed is not valid",
			args: []string{"invalidmode"},
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(fillRpkConfig(wdConfigPath, config.ModeDev))
				if err != nil {
					return "", err
				}
				return wdConfigPath, afero.WriteFile(fs, wdConfigPath, bs, 0644)
			},
			expectedOutput: "",
			expectedErrMsg: "'invalidmode' is not a supported mode. Available modes: dev, development, prod, production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			path, err := tt.before(fs)
			require.NoError(t, err)
			var out bytes.Buffer
			cmd := NewModeCommand(fs)
			cmd.SetArgs(tt.args)
			logrus.SetOutput(&out)
			err = cmd.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(t, err, tt.expectedErrMsg)
				return
			}
			require.NoError(t, err)
			output := out.String()
			require.Contains(t, strings.TrimSpace(output), tt.expectedOutput)
			conf, err := config.ReadConfigFromPath(fs, path)
			require.NoError(t, err)
			require.Exactly(t, tt.expectedConfig, *conf)
		})
	}
}
