package cmd

import (
	"bytes"
	"fmt"
	"os"
	"reflect"
	"testing"
	"vectorized/pkg/config"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

func getValidConfig(configFile string, rpkFill bool) config.Config {
	rpk := fillRpkConfig(rpkFill)
	return config.Config{
		ConfigFile: configFile,
		Redpanda: &config.RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: config.SocketAddress{
				Port:    33145,
				Address: "127.0.0.1",
			},
			Id: 1,
			KafkaApi: config.SocketAddress{
				Port:    9092,
				Address: "127.0.0.1",
			},
			SeedServers: []*config.SeedServer{
				&config.SeedServer{
					Host: config.SocketAddress{
						Port:    33145,
						Address: "127.0.0.1",
					},
					Id: 1,
				},
				&config.SeedServer{
					Host: config.SocketAddress{
						Port:    33146,
						Address: "127.0.0.1",
					},
					Id: 2,
				},
			},
		},
		Rpk: &rpk,
	}
}

func fillRpkConfig(val bool) config.RpkConfig {
	return config.RpkConfig{
		TuneNetwork:         val,
		TuneDiskScheduler:   val,
		TuneNomerges:        val,
		TuneDiskIrq:         val,
		TuneCpu:             val,
		TuneAioEvents:       val,
		TuneClocksource:     val,
		EnableMemoryLocking: val,
		TuneCoredump:        val,
		CoredumpDir:         "/redpanda/coredumps/",
	}
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
		fs             afero.Fs
		before         func(afero.Fs) (string, error)
		expectedConfig config.Config
		expectedOutput string
		expectedErrMsg string
	}{
		{
			name: "development mode should disable all fields in the rpk config",
			args: []string{"development", "--redpanda-cfg", configPath},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(configPath, true))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: getValidConfig(configPath, false),
			expectedOutput: fmt.Sprintf("Writing 'development' mode defaults to '%s'\n", configPath),
			expectedErrMsg: "",
		},
		{
			name: "production mode should enable all fields in the rpk config",
			args: []string{"production", "--redpanda-cfg", configPath},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(configPath, false))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: getValidConfig(configPath, true),
			expectedOutput: fmt.Sprintf("Writing 'production' mode defaults to '%s'\n", configPath),
			expectedErrMsg: "",
		},
		{
			name: "the development mode alias, 'dev', should work the same",
			args: []string{"dev", "--redpanda-cfg", configPath},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(configPath, true))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: getValidConfig(configPath, false),
			expectedOutput: fmt.Sprintf("Writing 'dev' mode defaults to '%s'\n", configPath),
			expectedErrMsg: "",
		},
		{
			name: "the production mode alias, 'prod', should work the same",
			args: []string{"prod", "--redpanda-cfg", configPath},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(configPath, false))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: getValidConfig(configPath, true),
			expectedOutput: fmt.Sprintf("Writing 'prod' mode defaults to '%s'\n", configPath),
			expectedErrMsg: "",
		},
		{
			name: "mode should work if --redpanda-cfg isn't passed, but the file is in /etc/redpanda/redpanda.yaml",
			args: []string{"prod"},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(configPath, false))
				if err != nil {
					return "", err
				}
				return configPath, afero.WriteFile(fs, configPath, bs, 0644)
			},
			expectedConfig: getValidConfig(configPath, true),
			expectedOutput: fmt.Sprintf("Writing 'prod' mode defaults to '%s'\n", configPath),
			expectedErrMsg: "",
		},
		{
			name: "mode should work if --redpanda-cfg isn't passed, but the file is in the current dir",
			args: []string{"development"},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(wdConfigPath, false))
				if err != nil {
					return "", err
				}
				return wdConfigPath, afero.WriteFile(fs, wdConfigPath, bs, 0644)
			},
			expectedConfig: getValidConfig(wdConfigPath, false),
			expectedOutput: (func() string {
				dir, _ := os.Getwd()
				return fmt.Sprintf("Writing 'development' mode defaults to '%s/redpanda.yaml'\n", dir)
			})(),
			expectedErrMsg: "",
		},
		{
			name: "mode lists the available modes if the one passed is not valid",
			args: []string{"invalidmode"},
			fs:   afero.NewMemMapFs(),
			before: func(fs afero.Fs) (string, error) {
				bs, err := yaml.Marshal(getValidConfig(wdConfigPath, false))
				if err != nil {
					return "", err
				}
				return wdConfigPath, afero.WriteFile(fs, wdConfigPath, bs, 0644)
			},
			expectedConfig: getValidConfig(wdConfigPath, false),
			expectedOutput: "",
			expectedErrMsg: "invalidmode is not a supported mode. Available modes: dev, prod, development, production",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path, err := tt.before(tt.fs)
			if err != nil {
				t.Errorf("got an error while setting up the test: %v", err)
			}
			var out bytes.Buffer
			cmd := NewModeCommand(tt.fs)
			cmd.SetArgs(tt.args)
			logrus.SetOutput(&out)
			err = cmd.Execute()
			if tt.expectedErrMsg != "" && tt.expectedErrMsg != err.Error() {
				t.Errorf("expected error message:\n%v\ngot:\n%v", tt.expectedErrMsg, err.Error())
			}
			output := out.String()
			if tt.expectedOutput != output {
				t.Errorf("expected output:\n\"%v\"\ngot:\n\"%v\"", tt.expectedOutput, output)
			}
			conf, err := config.ReadConfigFromPath(tt.fs, path)
			if err != nil {
				t.Errorf("got an unexpected error while reading the %s: %v", configPath, err)
			}
			if !reflect.DeepEqual(conf, &tt.expectedConfig) {
				t.Errorf("got %v, expected %v", conf, tt.expectedConfig)
			}
		})
	}
}
