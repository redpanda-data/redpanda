package config

import (
	"strings"
	"testing"

	"github.com/spf13/afero"
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
			exp: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
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
    enable_usage_stats: false
`,
		},
		{
			name: "write loaded config",
			inCfg: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
    data_directory: ""
    node_id: 1
    rack: my_rack
`,
			cfgChanges: func(c *Config) *Config {
				c.Redpanda.ID = 6
				return c
			},
			exp: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
    data_directory: ""
    node_id: 6
    rack: my_rack
`,
		},
		{
			name: "write empty structs",
			inCfg: `config_file: /etc/redpanda/redpanda.yaml
rpk:
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
			path := cfg.loadedPath
			if path == "" {
				path = Default().ConfigFile
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
