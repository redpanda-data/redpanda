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
            - 127.0.0.1:9092
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
				c.Redpanda.ID = new(int)
				*c.Redpanda.ID = 6
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
		{
			name: "preserve order of admin_api.addresses",
			inCfg: `rpk:
    admin_api:
        addresses:
            - localhost:4444
            - 127.0.0.1:4444
            - 10.0.0.1:4444
            - 122.65.33.12:4444
`,
			cfgChanges: func(c *Config) *Config {
				c.Rpk.KafkaAPI.Brokers = []string{"127.0.1.1:9647"}
				return c
			},
			exp: `rpk:
    kafka_api:
        brokers:
            - 127.0.1.1:9647
    admin_api:
        addresses:
            - localhost:4444
            - 127.0.0.1:4444
            - 10.0.0.1:4444
            - 122.65.33.12:4444
`,
		},
		{
			name: "don't rewrite if the content didn't changed",
			inCfg: `redpanda:
    seed_servers: []
    data_directory: /var/lib/redpanda/data
    rpc_server:
        port: 33145
        address: 0.0.0.0
rpk:
    admin_api:
         addresses:
             - 127.0.0.1:9644
    kafka_api:
         brokers:
             - 127.0.0.1:9092
`,
			exp: `redpanda:
    seed_servers: []
    data_directory: /var/lib/redpanda/data
    rpc_server:
        port: 33145
        address: 0.0.0.0
rpk:
    admin_api:
         addresses:
             - 127.0.0.1:9644
    kafka_api:
         brokers:
             - 127.0.0.1:9092
`,
		},
		{
			name: "rewrite if the content didn't changed but seed_server was using the old version",
			inCfg: `redpanda:
    seed_servers:
      - host:
        address: 0.0.0.0
        port: 33145
    data_directory: /var/lib/redpanda/data
    rpc_server:
        port: 33145
        address: 0.0.0.0
rpk:
    admin_api:
         addresses:
             - 127.0.0.1:9644
    kafka_api:
         brokers:
             - 127.0.0.1:9092
`,
			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    seed_servers:
        - host:
            address: 0.0.0.0
            port: 33145
    rpc_server:
        address: 0.0.0.0
        port: 33145
rpk:
    kafka_api:
        brokers:
            - 127.0.0.1:9092
    admin_api:
        addresses:
            - 127.0.0.1:9644
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
			ID:            nil,
			SeedServers:   []SeedServer{},
			DeveloperMode: true,
		},
		Rpk: RpkNodeConfig{
			Tuners: RpkNodeTuners{
				CoredumpDir: "/var/lib/redpanda/coredump",
			},
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
	cfg.rawFile = nil          // we don't want to compare the in-memory raw file
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
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`, string(file))
}

func TestAddUnsetDefaults(t *testing.T) {
	for _, test := range []struct {
		name   string
		inCfg  *Config
		expCfg *Config
	}{
		{
			name:  "default kafka broker and default admin api",
			inCfg: &Config{},
			expCfg: &Config{
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"127.0.0.1:9092"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"127.0.0.1:9644"},
					},
				},
			},
		},

		{
			name: "rpk configuration left alone if present",
			inCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "250.12.12.12", Port: 9095},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "0.0.2.3", Port: 4444},
					},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"foo:9092"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"bar:9644"},
					},
				},
			},
			expCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "250.12.12.12", Port: 9095},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "0.0.2.3", Port: 4444},
					},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"foo:9092"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"bar:9644"},
					},
				},
			},
		},

		{
			name: "kafka broker and admin api from redpanda",
			inCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "250.12.12.12", Port: 9095},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "0.0.2.3", Port: 4444},
					},
				},
			},
			expCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "250.12.12.12", Port: 9095},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "0.0.2.3", Port: 4444},
					},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"250.12.12.12:9095"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"0.0.2.3:4444"},
					},
				},
			},
		},

		{
			name: "admin api sorted, no TLS used because we have non-TLS servers",
			inCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 5555, Name: "tls"},     // private, TLS
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},    // loopback, TLS
						{Address: "localhost", Port: 5555, Name: "tls"},    // localhost, TLS
						{Address: "122.61.33.12", Port: 5555, Name: "tls"}, // public, TLS
						{Address: "10.1.2.1", Port: 9999},                  // private
						{Address: "127.1.2.1", Port: 9999},                 // loopback
						{Address: "localhost", Port: 9999},                 // localhost
						{Address: "122.61.32.12", Port: 9999},              // public
						{Address: "0.0.0.0", Port: 9999},                   // rewritten to 127.0.0.1
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
					AdminAPI: []NamedSocketAddress{
						{Address: "10.0.0.1", Port: 4444, Name: "tls"}, // same as above, numbers in addr/port slightly changed
						{Address: "127.0.0.1", Port: 4444, Name: "tls"},
						{Address: "localhost", Port: 4444, Name: "tls"},
						{Address: "122.65.33.12", Port: 4444, Name: "tls"},
						{Address: "10.0.2.1", Port: 7777},
						{Address: "127.0.2.1", Port: 7777},
						{Address: "localhost", Port: 7777},
						{Address: "122.65.32.12", Port: 7777},
					},
					AdminAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
			},
			expCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 5555, Name: "tls"},
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
						{Address: "localhost", Port: 5555, Name: "tls"},
						{Address: "122.61.33.12", Port: 5555, Name: "tls"},
						{Address: "10.1.2.1", Port: 9999},
						{Address: "127.1.2.1", Port: 9999},
						{Address: "localhost", Port: 9999},
						{Address: "122.61.32.12", Port: 9999},
						{Address: "0.0.0.0", Port: 9999},
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
					AdminAPI: []NamedSocketAddress{
						{Address: "10.0.0.1", Port: 4444, Name: "tls"},
						{Address: "127.0.0.1", Port: 4444, Name: "tls"},
						{Address: "localhost", Port: 4444, Name: "tls"},
						{Address: "122.65.33.12", Port: 4444, Name: "tls"},
						{Address: "10.0.2.1", Port: 7777},
						{Address: "127.0.2.1", Port: 7777},
						{Address: "localhost", Port: 7777},
						{Address: "122.65.32.12", Port: 7777},
					},
					AdminAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{
							"localhost:9999",
							"127.1.2.1:9999",
							"127.0.0.1:9999",
							"10.1.2.1:9999",
							"122.61.32.12:9999",
						},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{
							"localhost:7777",    // localhost
							"127.0.2.1:7777",    // loopback
							"10.0.2.1:7777",     // private
							"122.65.32.12:7777", // public
						},
					},
				},
			},
		},

		{
			name: "broker and admin api sorted with TLS and MTLS",
			inCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 1111, Name: "mtls"}, // similar to above test
						{Address: "127.1.0.1", Port: 1111, Name: "mtls"},
						{Address: "localhost", Port: 1111, Name: "mtls"},
						{Address: "122.61.33.12", Port: 1111, Name: "mtls"},
						{Address: "10.1.0.1", Port: 5555, Name: "tls"},
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
						{Address: "localhost", Port: 5555, Name: "tls"},
						{Address: "122.61.33.12", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{
						{Name: "tls", Enabled: true},
						{Name: "mtls", Enabled: true, RequireClientAuth: true},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "10.1.0.9", Port: 2222, Name: "mtls"},
						{Address: "127.1.0.9", Port: 2222, Name: "mtls"},
						{Address: "localhost", Port: 2222, Name: "mtls"},
						{Address: "122.61.33.9", Port: 2222, Name: "mtls"},
						{Address: "10.1.0.9", Port: 4444, Name: "tls"},
						{Address: "127.1.0.9", Port: 4444, Name: "tls"},
						{Address: "localhost", Port: 4444, Name: "tls"},
						{Address: "122.61.33.9", Port: 4444, Name: "tls"},
					},
					AdminAPITLS: []ServerTLS{
						{Name: "mtls", Enabled: true, RequireClientAuth: true},
						{Name: "tls", Enabled: true},
					},
				},
			},
			expCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 1111, Name: "mtls"},
						{Address: "127.1.0.1", Port: 1111, Name: "mtls"},
						{Address: "localhost", Port: 1111, Name: "mtls"},
						{Address: "122.61.33.12", Port: 1111, Name: "mtls"},
						{Address: "10.1.0.1", Port: 5555, Name: "tls"},
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
						{Address: "localhost", Port: 5555, Name: "tls"},
						{Address: "122.61.33.12", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{
						{Name: "tls", Enabled: true},
						{Name: "mtls", Enabled: true, RequireClientAuth: true},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "10.1.0.9", Port: 2222, Name: "mtls"},
						{Address: "127.1.0.9", Port: 2222, Name: "mtls"},
						{Address: "localhost", Port: 2222, Name: "mtls"},
						{Address: "122.61.33.9", Port: 2222, Name: "mtls"},
						{Address: "10.1.0.9", Port: 4444, Name: "tls"},
						{Address: "127.1.0.9", Port: 4444, Name: "tls"},
						{Address: "localhost", Port: 4444, Name: "tls"},
						{Address: "122.61.33.9", Port: 4444, Name: "tls"},
					},
					AdminAPITLS: []ServerTLS{
						{Name: "mtls", Enabled: true, RequireClientAuth: true},
						{Name: "tls", Enabled: true},
					},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{
							"localhost:5555",
							"127.1.0.1:5555",
							"10.1.0.1:5555",
							"122.61.33.12:5555",
						},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{
							"localhost:4444",
							"127.1.0.9:4444",
							"10.1.0.9:4444",
							"122.61.33.9:4444",
						},
					},
				},
			},
		},

		{
			name: "broker and admin api sorted with TLS",
			inCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 5555, Name: "tls"},
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
						{Address: "localhost", Port: 5555, Name: "tls"},
						{Address: "122.61.33.12", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
					AdminAPI: []NamedSocketAddress{
						{Address: "10.0.0.1", Port: 4444, Name: "tls"},
						{Address: "127.0.0.1", Port: 4444, Name: "tls"},
						{Address: "localhost", Port: 4444, Name: "tls"},
						{Address: "122.65.33.12", Port: 4444, Name: "tls"},
					},
					AdminAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
			},
			expCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 5555, Name: "tls"},
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
						{Address: "localhost", Port: 5555, Name: "tls"},
						{Address: "122.61.33.12", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
					AdminAPI: []NamedSocketAddress{
						{Address: "10.0.0.1", Port: 4444, Name: "tls"},
						{Address: "127.0.0.1", Port: 4444, Name: "tls"},
						{Address: "localhost", Port: 4444, Name: "tls"},
						{Address: "122.65.33.12", Port: 4444, Name: "tls"},
					},
					AdminAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{
							"localhost:5555",
							"127.1.0.1:5555",
							"10.1.0.1:5555",
							"122.61.33.12:5555",
						},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{
							"localhost:4444",
							"127.0.0.1:4444",
							"10.0.0.1:4444",
							"122.65.33.12:4444",
						},
					},
				},
			},
		},

		{
			name: "assume the admin API when only Kafka API is available",
			inCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
			},
			expCfg: &Config{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{
							"127.1.0.1:5555",
						},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{
							"127.1.0.1:9644",
						},
					},
				},
			},
		},

		{
			name: "assume the Kafka API API when only admin API is available from rpk with TLS",
			inCfg: &Config{
				Rpk: RpkNodeConfig{
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"127.1.0.1:5555"},
						TLS:       new(TLS),
					},
				},
			},
			expCfg: &Config{
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"127.1.0.1:9092"},
						TLS:     new(TLS),
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"127.1.0.1:5555"},
						TLS:       new(TLS),
					},
				},
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			test.inCfg.addUnsetDefaults()
			require.Equal(t, test.expCfg, test.inCfg)
		})
	}
}
