package coredump

import (
	"os"
	"testing"
	"vectorized/pkg/redpanda"
	"vectorized/pkg/tuners/executors"

	"github.com/spf13/afero"
)

func validConfig() redpanda.Config {
	return redpanda.Config{
		Redpanda: &redpanda.RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: redpanda.SocketAddress{
				Port:    33145,
				Address: "127.0.0.1",
			},
			Id: 1,
			KafkaApi: redpanda.SocketAddress{
				Port:    9092,
				Address: "127.0.0.1",
			},
			SeedServers: []*redpanda.SeedServer{
				&redpanda.SeedServer{
					Host: redpanda.SocketAddress{
						Port:    33145,
						Address: "127.0.0.1",
					},
					Id: 1,
				},
				&redpanda.SeedServer{
					Host: redpanda.SocketAddress{
						Port:    33146,
						Address: "127.0.0.1",
					},
					Id: 2,
				},
			},
		},
		Rpk: &redpanda.RpkConfig{
			TuneNetwork:         true,
			TuneDiskScheduler:   true,
			TuneNomerges:        true,
			TuneDiskIrq:         true,
			TuneCpu:             true,
			TuneAioEvents:       true,
			TuneClocksource:     true,
			EnableMemoryLocking: true,
			TuneCoredump:        true,
			CoredumpDir:         "/var/lib/redpanda/coredumps",
		},
	}
}

func TestTune(t *testing.T) {
	type args struct {
		fs     afero.Fs
		config func() redpanda.Config
	}
	tests := []struct {
		name string
		pre  func(afero.Fs, redpanda.Config) error
		args args
	}{
		{
			name: "it should install the coredump config file",
			args: args{
				fs:     afero.NewMemMapFs(),
				config: validConfig,
			},
		},
		{
			name: "it should not fail to install if the coredump config file already exists",
			pre: func(fs afero.Fs, config redpanda.Config) error {
				_, err := fs.Create(corePatternFilePath)
				return err
			},
			args: args{
				fs:     afero.NewMemMapFs(),
				config: validConfig,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			config := tt.args.config()
			if tt.pre != nil {
				if err := tt.pre(tt.args.fs, config); err != nil {
					t.Errorf("got an unexpected error while setting up the test: %v", err)
				}
			}

			tuner := NewCoredumpTuner(tt.args.fs, tt.args.config(), executors.NewDirectExecutor())
			res := tuner.Tune()
			if res.Error() != nil {
				t.Errorf("Tune() returned an unexpected error: %v", res.Error())
			}
			pattern, err := tt.args.fs.Open(corePatternFilePath)
			if err != nil {
				msg := "an unexpected error happened while trying to open the pattern file: %v"
				t.Errorf(msg, err)
			}
			script, err := tt.args.fs.Open(scriptFilePath)
			if err != nil {
				msg := "an unexpected error happened while trying to open the script: %v"
				t.Errorf(msg, err)
			}
			info, err := script.Stat()
			if err != nil {
				msg := "an unexpected error happened while trying to stat the script: %v"
				t.Errorf(msg, err)
			}
			// Check that the script is world-readable, writable and executable
			expectedMode := os.FileMode(int(0777))
			if info.Mode() != expectedMode {
				t.Errorf(
					"expected the script file mode to be %v, got %v",
					expectedMode,
					info.Mode(),
				)
			}
			expectedScript, err := renderTemplate(coredumpScriptTmpl, *config.Rpk)
			if err != nil {
				msg := "an unexpected error happened while rendering the script: %v"
				t.Errorf(msg, err)
			}
			buf := make([]byte, len(expectedScript))
			if _, err = script.Read(buf); err != nil {
				msg := "an unexpected error happened while trying to read the script: %v"
				t.Errorf(msg, err)
			}
			actualScript := string(buf)
			if actualScript != expectedScript {
				t.Errorf("expected '%s', got '%s", expectedScript, actualScript)
			}
			buf = make([]byte, len(coredumpPattern))
			if _, err = pattern.Read(buf); err != nil {
				msg := "an unexpected error happened while trying to read the file: %v"
				t.Errorf(msg, err)
			}
			actualPattern := string(buf)
			if actualPattern != coredumpPattern {
				t.Errorf("expected '%s', got '%s", coredumpPattern, actualPattern)
			}
		})
	}
}
