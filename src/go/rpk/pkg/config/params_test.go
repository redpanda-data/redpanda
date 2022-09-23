package config

import (
	"os"
	"strings"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestParams_Write(t *testing.T) {
	tests := []struct {
		name       string
		inCfg      string
		cfgChanges func(*Config) *Config
		exp        string
		expErr     bool
	}{
		{
			name: "create default config file if there is no config file yet",
			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    node_id: 0
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    kafka_api:
        brokers:
            - 0.0.0.0:9092
    admin_api:
        addresses:
            - 127.0.0.1:9644
`,
		},
		{
			name: "write loaded config",
			inCfg: `redpanda:
    data_directory: ""
    node_id: 1
    rack: my_rack
`,
			cfgChanges: func(c *Config) *Config {
				c.Redpanda.ID = 6
				return c
			},
			exp: `redpanda:
    node_id: 6
    rack: my_rack
`,
		},
		{
			name: "write empty structs",
			inCfg: `rpk:
    tls:
        truststore_file: ""
        cert_file: ""
        key_file: ""
`,
			cfgChanges: func(c *Config) *Config {
				c.Rpk.KafkaAPI.Brokers = []string{"127.0.1.1:9647"}
				return c
			},
			exp: `rpk:
    tls: {}
    kafka_api:
        brokers:
            - 127.0.1.1:9647
`,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			if test.inCfg != "" {
				// We assume for this test that all files will be in the default location.
				err := afero.WriteFile(fs, "/etc/redpanda/redpanda.yaml", []byte(test.inCfg), 0o644)
				if err != nil {
					t.Errorf("unexpected error while writing initial config file: %s", err)
					return
				}
			}
			cfg, err := new(Params).Load(fs)
			if err != nil {
				t.Errorf("unexpected error while loading config file: %s", err)
				return
			}

			// We use the loaded filepath, or the default in-mem generated
			// config path if no file was loaded.
			path := cfg.fileLocation
			if path == "" {
				path = DefaultPath
			}

			if test.cfgChanges != nil {
				cfg = test.cfgChanges(cfg)
			}

			err = cfg.Write(fs)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err? %v, exp err? %v; error: %v", gotErr, test.expErr, err)
				return
			}

			b, err := afero.ReadFile(fs, path)
			if err != nil {
				t.Errorf("unexpected error while reading the file in %s", path)
				return
			}
			if !strings.Contains(string(b), test.exp) {
				t.Errorf("string:\n%v, does not contain expected:\n%v", string(b), test.exp)
				return
			}
		})
	}
}

func TestRedpandaSampleFile(t *testing.T) {
	// Config from 'redpanda/conf/redpanda.yaml'.
	sample, err := os.ReadFile("../../../../../conf/redpanda.yaml")
	if err != nil {
		t.Errorf("unexpected error while reading sample config file: %s", err)
		return
	}
	fs := afero.NewMemMapFs()
	err = afero.WriteFile(fs, "/etc/redpanda/redpanda.yaml", sample, 0o644)
	if err != nil {
		t.Errorf("unexpected error while writing sample config file: %s", err)
		return
	}
	expCfg := &Config{
		fileLocation: "/etc/redpanda/redpanda.yaml",
		Redpanda: RedpandaNodeConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{
				Address: "0.0.0.0",
				Port:    33145,
			},
			KafkaAPI: []NamedAuthNSocketAddress{{
				Address: "0.0.0.0",
				Port:    9092,
			}},
			AdminAPI: []NamedSocketAddress{{
				Address: "0.0.0.0",
				Port:    9644,
			}},
			ID:            1,
			SeedServers:   []SeedServer{},
			DeveloperMode: true,
		},
		Rpk: RpkConfig{
			CoredumpDir:      "/var/lib/redpanda/coredump",
			EnableUsageStats: true,
		},
		Pandaproxy:     &Pandaproxy{},
		SchemaRegistry: &SchemaRegistry{},
	}
	// Load and check we load it correctly
	cfg, err := new(Params).Load(fs)
	if err != nil {
		t.Errorf("unexpected error while loading sample config file: %s", err)
		return
	}
	cfg = cfg.FileOrDefaults() // we want to check that we correctly load the raw file
	require.Equal(t, expCfg, cfg)

	// Write to the file and check we don't mangle the config properties
	err = cfg.Write(fs)
	if err != nil {
		t.Errorf("unexpected error while writing config file: %s", err)
		return
	}
	file, err := afero.ReadFile(fs, "/etc/redpanda/redpanda.yaml")
	if err != nil {
		t.Errorf("unexpected error while reading config file from fs: %s", err)
		return
	}
	require.Equal(t, `redpanda:
    data_directory: /var/lib/redpanda/data
    node_id: 1
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    enable_usage_stats: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`, string(file))
}
