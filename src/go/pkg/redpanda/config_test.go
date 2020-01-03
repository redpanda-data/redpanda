package redpanda

import (
	"fmt"
	"reflect"
	"strings"
	"testing"
	"vectorized/pkg/utils"

	vyaml "vectorized/pkg/yaml"

	"github.com/spf13/afero"
	"gopkg.in/yaml.v2"
)

func getValidConfig() *Config {
	return &Config{
		Redpanda: &RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{
				Port:    33145,
				Address: "127.0.0.1",
			},
			Id: 1,
			KafkaApi: SocketAddress{
				Port:    9092,
				Address: "127.0.0.1",
			},
			SeedServers: []*SeedServer{
				&SeedServer{
					Host: SocketAddress{
						Port:    33145,
						Address: "127.0.0.1",
					},
					Id: 1,
				},
				&SeedServer{
					Host: SocketAddress{
						Port:    33146,
						Address: "127.0.0.1",
					},
					Id: 2,
				},
			},
		},
		Rpk: &RpkConfig{
			TuneNetwork:         true,
			TuneDiskScheduler:   true,
			TuneNomerges:        true,
			TuneDiskIrq:         true,
			TuneCpu:             true,
			TuneAioEvents:       true,
			TuneClocksource:     true,
			TuneSwappiness:      true,
			EnableMemoryLocking: true,
			TuneCoredump:        true,
			CoredumpDir:         "/var/lib/redpanda/coredumps",
			WellKnownIo:         "vendor:vm:storage",
		},
	}
}

func TestReadConfigFromPath(t *testing.T) {
	const baseDir string = "/etc/redpanda"
	type args struct {
		fs   afero.Fs
		path string
	}
	tests := []struct {
		name    string
		args    args
		before  func(afero.Fs, string) error
		want    func() *Config
		wantErr bool
	}{
		{
			name: "shall return a config struct filled with values from the file",
			before: func(fs afero.Fs, path string) error {
				bs, err := yaml.Marshal(getValidConfig())
				if err != nil {
					return err
				}
				if err = fs.MkdirAll(baseDir, 0755); err != nil {
					return err
				}
				_, err = utils.WriteBytes(fs, bs, path)
				return err
			},
			args: args{
				fs:   afero.NewMemMapFs(),
				path: baseDir + "/redpanda.yaml",
			},
			want:    getValidConfig,
			wantErr: false,
		},
		// TODO: Add tests for when the config file has missing objects
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.before(tt.args.fs, tt.args.path); err != nil {
				t.Fatalf("got an error while setting up %v: %v", tt.name, err)
			}
			got, err := ReadConfigFromPath(tt.args.fs, tt.args.path)
			want := tt.want()
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadConfigFromPath() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, want) {
				t.Errorf("ReadConfigFromPath() = %v, want %v", *got, *want)
			}
		})
	}
}

func TestWriteConfig(t *testing.T) {
	const path string = "/redpanda.yaml"
	type args struct {
		config func() *Config
		fs     afero.Fs
		path   string
	}
	tests := []struct {
		name     string
		args     args
		wantErr  bool
		expected string
	}{
		{
			name: "shall write a valid config file without an rpk config object",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: path,
				config: func() *Config {
					c := getValidConfig()
					c.Rpk = nil
					return c
				},
			},
			wantErr: false,
			expected: `redpanda:
  data_directory: /var/lib/redpanda/data
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 127.0.0.1
    port: 9092
  node_id: 1
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
    node_id: 1
  - host:
      address: 127.0.0.1
      port: 33146
    node_id: 2
`,
		},
		{
			name: "shall fail with an invalid config",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: path,
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.Directory = ""
					return c
				},
			},
			wantErr: true,
		},
		{
			name: "shall fail if there's no redpanda config",
			args: args{
				fs:   afero.NewMemMapFs(),
				path: path,
				config: func() *Config {
					return &Config{}
				},
			},
			wantErr: true,
		},
		{
			name: "shall write a valid config file with an rpk config object",
			args: args{
				fs:     afero.NewMemMapFs(),
				path:   path,
				config: getValidConfig,
			},
			wantErr: false,
			expected: `redpanda:
  data_directory: /var/lib/redpanda/data
  rpc_server:
    address: 127.0.0.1
    port: 33145
  kafka_api:
    address: 127.0.0.1
    port: 9092
  node_id: 1
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
    node_id: 1
  - host:
      address: 127.0.0.1
      port: 33146
    node_id: 2
rpk:
  tune_network: true
  tune_disk_scheduler: true
  tune_disk_nomerges: true
  tune_disk_irq: true
  tune_cpu: true
  tune_aio_events: true
  tune_clocksource: true
  tune_swappiness: true
  enable_memory_locking: true
  tune_coredump: true
  coredump_dir: /var/lib/redpanda/coredumps
  well_known_io: vendor:vm:storage
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// WriteConfig needs the file at the given path to exist.
			err := vyaml.Persist(tt.args.fs, tt.args.config(), tt.args.path)
			if err != nil {
				t.Fatal(err.Error())
			}
			err = WriteConfig(tt.args.fs, tt.args.config(), tt.args.path)
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected an error, got nil")
				}
				return
			} else if err != nil {
				t.Fatalf("got an unexpected error: %v", err)
			}

			contentBytes, err := afero.ReadFile(tt.args.fs, path)
			if err != nil {
				t.Errorf("got an error while reading %v: %v", tt.args.path, err)
			}
			content := string(contentBytes)
			if content != tt.expected {
				t.Errorf("Expected:\n'%s'\n Got:\n'%s'\n content differs",
					strings.ReplaceAll(tt.expected, " ", "·"),
					strings.ReplaceAll(content, " ", "·"),
				)
			}
			backup := fmt.Sprintf("%s.bk", tt.args.path)
			_, err = tt.args.fs.Stat(backup)
			if err != nil {
				t.Errorf("got an error while stat'ing %v: %v", backup, err)
			}
		})
	}
}

func TestCheckConfig(t *testing.T) {
	type args struct {
		config func() *Config
	}
	tests := []struct {
		name     string
		args     args
		expected []string
	}{
		{
			name: "shall return no errors when config is valid",
			args: args{
				config: getValidConfig,
			},
			expected: []string{},
		},
		{
			name: "shall return an error when config file does not contain data directory setting",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.Directory = ""
					return c
				},
			},
			expected: []string{"redpanda.data_directory can't be empty"},
		},
		{
			name: "shall return an error when id of server is negative",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.Id = -100
					return c
				},
			},
			expected: []string{"redpanda.id can't be a negative integer"},
		},
		{
			name: "shall return an error when the RPC server port is 0",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.RPCServer.Port = 0
					return c
				},
			},
			expected: []string{"redpanda.rpc_server.port can't be 0"},
		},
		{
			name: "shall return an error when the RPC server address is empty",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.RPCServer.Address = ""
					return c
				},
			},
			expected: []string{"redpanda.rpc_server.address can't be empty"},
		},
		{
			name: "shall return an error when the Kafka API port is 0",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.KafkaApi.Port = 0
					return c
				},
			},
			expected: []string{"redpanda.kafka_api.port can't be 0"},
		},
		{
			name: "shall return an error when the Kafka API address is empty",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.KafkaApi.Address = ""
					return c
				},
			},
			expected: []string{"redpanda.kafka_api.address can't be empty"},
		},
		{
			name: "shall return an error when the seed servers list is empty",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.SeedServers = []*SeedServer{}
					return c
				},
			},
			expected: []string{"redpanda.seed_servers can't be empty"},
		},
		{
			name: "shall return an error when one of the seed servers' address is empty",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.SeedServers[0].Host.Address = ""
					return c
				},
			},
			expected: []string{"redpanda.seed_servers.0.host.address can't be empty"},
		},
		{
			name: "shall return an error when one of the seed servers' port is 0",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Redpanda.SeedServers[1].Host.Port = 0
					return c
				},
			},
			expected: []string{"redpanda.seed_servers.1.host.port can't be 0"},
		},
		{
			name: "shall return no errors when tune_coredump is set to false," +
				"regardless of coredump_dir's value",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Rpk.TuneCoredump = false
					c.Rpk.CoredumpDir = ""
					return c
				},
			},
			expected: []string{},
		},
		{
			name: "shall return an error when tune_coredump is set to true," +
				"but coredump_dir is empty",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Rpk.CoredumpDir = ""
					return c
				},
			},
			expected: []string{"if rpk.tune_coredump is set to true," +
				"rpk.coredump_dir can't be empty"},
		},
		{
			name: "shall return no error if setup is empty," +
				"but coredump_dir is empty",
			args: args{
				config: func() *Config {
					c := getValidConfig()
					c.Rpk.WellKnownIo = ""
					return c
				},
			},
			expected: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := CheckConfig(tt.args.config())
			if len(got) != len(tt.expected) {
				t.Fatalf("got a different amount of errors than expected: got: %v expected: %v", got, tt.expected)
			}
			for _, errMsg := range tt.expected {
				present := false
				for _, err := range got {
					present = present || errMsg == err.Error()
				}
				if !present {
					t.Errorf("expected error msg \"%v\" wasn't among the result error set %v", errMsg, got)
				}
			}
		})
	}
}
