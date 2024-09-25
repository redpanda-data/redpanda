package config

import (
	"os"
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/testfs"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"golang.org/x/exp/maps"
)

func TestParams_RedpandaYamlWrite(t *testing.T) {
	tests := []struct {
		name   string
		inCfg  string
		mutate func(*RedpandaYaml)
		exp    string
		expErr bool
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
			mutate: func(c *RedpandaYaml) {
				c.Redpanda.ID = new(int)
				*c.Redpanda.ID = 6
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
			mutate: func(c *RedpandaYaml) {
				c.Rpk.KafkaAPI.Brokers = []string{"127.0.1.1:9647"}
			},
			exp: `rpk:
    kafka_api:
        brokers:
            - 127.0.1.1:9647
        tls: {}
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
			mutate: func(c *RedpandaYaml) {
				c.Rpk.KafkaAPI.Brokers = []string{"127.0.1.1:9647"}
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
			y := cfg.VirtualRedpandaYaml()

			if test.mutate != nil {
				test.mutate(y)
			}

			err = y.Write(fs)

			gotErr := err != nil
			if gotErr != test.expErr {
				t.Errorf("got err? %v, exp err? %v; error: %v", gotErr, test.expErr, err)
				return
			}

			b, err := afero.ReadFile(fs, y.fileLocation)
			if err != nil {
				t.Errorf("unexpected error while reading the file in %s", y.fileLocation)
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
	expCfg := &RedpandaYaml{
		fileLocation: "/etc/redpanda/redpanda.yaml",
		Redpanda: RedpandaNodeConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{
				Address: "0.0.0.0",
				Port:    33145,
			},
			AdvertisedRPCAPI: &SocketAddress{
				Address: "127.0.0.1",
				Port:    33145,
			},
			KafkaAPI: []NamedAuthNSocketAddress{{
				Address: "0.0.0.0",
				Port:    9092,
			}},
			AdvertisedKafkaAPI: []NamedSocketAddress{{
				Address: "127.0.0.1",
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
	y := cfg.ActualRedpandaYamlOrDefaults() // we want to check that we correctly load the raw file
	y.fileRaw = nil                         // we don't want to compare the in-memory raw file
	require.Equal(t, expCfg, y)

	// Write to the file and check we don't mangle the config properties
	err = y.Write(fs)
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
    advertised_rpc_api:
        address: 127.0.0.1
        port: 33145
    advertised_kafka_api:
        - address: 127.0.0.1
          port: 9092
    developer_mode: true
rpk:
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`, string(file))
}

func TestAddUnsetRedpandaDefaults(t *testing.T) {
	for _, test := range []struct {
		name   string
		inCfg  *RedpandaYaml
		expCfg *RedpandaYaml
	}{
		{
			name: "rpk configuration left alone if present",
			inCfg: &RedpandaYaml{
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
			expCfg: &RedpandaYaml{
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
			inCfg: &RedpandaYaml{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "250.12.12.12", Port: 9095},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "0.0.2.3", Port: 4444},
					},
				},
			},
			expCfg: &RedpandaYaml{
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
			inCfg: &RedpandaYaml{
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
			expCfg: &RedpandaYaml{
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
			inCfg: &RedpandaYaml{
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
			expCfg: &RedpandaYaml{
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
			inCfg: &RedpandaYaml{
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
			expCfg: &RedpandaYaml{
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
			inCfg: &RedpandaYaml{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "127.1.0.1", Port: 5555, Name: "tls"},
					},
					KafkaAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
			},
			expCfg: &RedpandaYaml{
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
			inCfg: &RedpandaYaml{
				Rpk: RpkNodeConfig{
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"127.1.0.1:5555"},
						TLS:       new(TLS),
					},
				},
			},
			expCfg: &RedpandaYaml{
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
			c := Config{
				redpandaYaml: *test.inCfg,
			}
			// We just want to check our field migrations work, so
			// we do not try to differentiate Virtual vs
			// actual here.
			c.addUnsetRedpandaDefaults(false)
			require.Equal(t, test.expCfg, &c.redpandaYaml)
		})
	}
}

func TestLoadRpkAndRedpanda(t *testing.T) {
	defaultRpkPath, err := DefaultRpkYamlPath()
	if err != nil {
		t.Fatalf("unable to load default rpk yaml path: %v", err)
	}
	for _, test := range []struct {
		name string

		redpandaYaml string
		rpkYaml      string

		expVirtualRedpanda string
		expVirtualRpk      string
	}{
		// If both are empty, we use the default rpk and redpanda
		// configurations. Some aspects of the redpanda config are
		// ported to the Virtual rpk config.
		{
			name: "both empty no config flag",
			expVirtualRedpanda: `redpanda:
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
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
			expVirtualRpk: `version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: default
current_cloud_auth_org_id: default-org-no-id
current_cloud_auth_kind: ""
profiles:
    - name: default
      description: Default rpk profile
      prompt: ""
      from_cloud: false
      kafka_api:
        brokers:
            - 127.0.0.1:9092
      admin_api:
        addresses:
            - 127.0.0.1:9644
      schema_registry:
        addresses:
            - 127.0.0.1:8081
cloud_auth:
    - name: default
      organization: Default organization
      org_id: default-org-no-id
      kind: ""
`,
		},

		// If only redpanda.yaml exists, it is mostly similar to both
		// being empty. Tuners and some other fields are ported to the
		// Virtual rpk.yaml.
		//
		// * developer_mode is not turned on since we do not use DevDefaults
		// * rpk uses redpanda's kafka_api
		// * rpk uses kafka_api + admin_port for admin_api
		// * rpk.yaml uses redpanda.yaml tuners
		{
			name: "redpanda.yaml exists",
			redpandaYaml: `redpanda:
    data_directory: /data
    seed_servers:
        - host:
            address: 127.0.0.1
            port: 33145
        - host:
            address: 127.0.0.1
            port: 33146
    kafka_api:
        - address: 0.0.0.3
          port: 9092
rpk:
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,

			expVirtualRedpanda: `redpanda:
    data_directory: /data
    seed_servers:
        - host:
            address: 127.0.0.1
            port: 33145
        - host:
            address: 127.0.0.1
            port: 33146
    kafka_api:
        - address: 0.0.0.3
          port: 9092
rpk:
    kafka_api:
        brokers:
            - 0.0.0.3:9092
    admin_api:
        addresses:
            - 0.0.0.3:9644
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,
			expVirtualRpk: `version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: default
current_cloud_auth_org_id: default-org-no-id
current_cloud_auth_kind: ""
profiles:
    - name: default
      description: Default rpk profile
      prompt: ""
      from_cloud: false
      kafka_api:
        brokers:
            - 0.0.0.3:9092
      admin_api:
        addresses:
            - 0.0.0.3:9644
      schema_registry:
        addresses:
            - 127.0.0.1:8081
cloud_auth:
    - name: default
      organization: Default organization
      org_id: default-org-no-id
      kind: ""
`,
		},

		// If only rpk.yaml exists, we port sections from it into
		// redpanda.yaml.
		//
		// * missing kafka port is defaulted to 9092
		// * admin api is defaulted, using kafka broker ip
		{
			name: "rpk.yaml exists",
			rpkYaml: `version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: foo
current_cloud_auth_org_id: fizz-org-id
current_cloud_auth_kind: sso
profiles:
    - name: foo
      description: descriptosphere
      prompt: ""
      from_cloud: false
      kafka_api:
        brokers:
            - 0.0.0.3
      admin_api: {}
      schema_registry:
        addresses:
            - 0.0.0.2
cloud_auth:
    - name: fizz
      organization: fizzy
      org_id: fizz-org-id
      kind: sso
`,

			expVirtualRedpanda: `redpanda:
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
            - 0.0.0.3:9092
    admin_api:
        addresses:
            - 0.0.0.3:9644
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
			expVirtualRpk: `version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: foo
current_cloud_auth_org_id: fizz-org-id
current_cloud_auth_kind: sso
profiles:
    - name: foo
      description: descriptosphere
      prompt: ""
      from_cloud: false
      kafka_api:
        brokers:
            - 0.0.0.3:9092
      admin_api:
        addresses:
            - 0.0.0.3:9644
      schema_registry:
        addresses:
            - 0.0.0.2:8081
cloud_auth:
    - name: fizz
      organization: fizzy
      org_id: fizz-org-id
      kind: sso
`,
		},

		// Note that we ignore the redpanda.yaml's redpanda.{kafka,admin}_api
		// because we pull data from rpk.yaml and then rely on defaults.
		//
		// * copy rpk.yaml kafka_api to redpanda.rpk.kafka_api
		// * port redpanda.yaml's rpk.kafka_api to rpk.admin_api hosts
		// * port redpanda.yaml's rpk.admin_api to rpk.yaml's
		// * copy redpanda.yaml tuners to rpk.yaml
		{
			name: "both yaml files exist",
			redpandaYaml: `redpanda:
    data_directory: /data
    seed_servers: []
    kafka_api:
        - address: 0.0.0.3
          port: 9097
    admin_api:
        - address: admin.com
          port: 4444
rpk:
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,
			rpkYaml: `version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: foo
current_cloud_auth_org_id: ""
current_cloud_auth_kind: ""
profiles:
    - name: foo
      description: descriptosphere
      prompt: ""
      from_cloud: false
      kafka_api:
        brokers:
            - 128.0.0.4
      admin_api: {}
      schema_registry: {}
cloud_auth: []
`,

			expVirtualRedpanda: `redpanda:
    data_directory: /data
    seed_servers: []
    kafka_api:
        - address: 0.0.0.3
          port: 9097
    admin_api:
        - address: admin.com
          port: 4444
rpk:
    kafka_api:
        brokers:
            - 128.0.0.4:9092
    admin_api:
        addresses:
            - 128.0.0.4:9644
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,

			expVirtualRpk: `version: 6
globals:
    prompt: ""
    no_default_cluster: false
    command_timeout: 0s
    dial_timeout: 0s
    request_timeout_overhead: 0s
    retry_timeout: 0s
    fetch_max_wait: 0s
    kafka_protocol_request_client_id: ""
current_profile: foo
current_cloud_auth_org_id: default-org-no-id
current_cloud_auth_kind: ""
profiles:
    - name: foo
      description: descriptosphere
      prompt: ""
      from_cloud: false
      kafka_api:
        brokers:
            - 128.0.0.4:9092
      admin_api:
        addresses:
            - 128.0.0.4:9644
      schema_registry:
        addresses:
            - 127.0.0.1:8081
cloud_auth:
    - name: default
      organization: Default organization
      org_id: default-org-no-id
      kind: ""
`,
		},

		//
	} {
		t.Run(test.name, func(t *testing.T) {
			m := make(map[string]testfs.Fmode)
			if test.redpandaYaml != "" {
				m[DefaultRedpandaYamlPath] = testfs.RFile(test.redpandaYaml)
			}
			if test.rpkYaml != "" {
				m[defaultRpkPath] = testfs.RFile(test.rpkYaml)
			}
			fs := testfs.FromMap(m)

			cfg, err := new(Params).Load(fs)
			if err != nil {
				t.Fatalf("unexpected err: %v", err)
			}

			{
				mat := cfg.VirtualRedpandaYaml()
				mat.Write(fs)
				m[DefaultRedpandaYamlPath] = testfs.RFile(test.expVirtualRedpanda)
			}
			{
				act, ok := cfg.ActualRedpandaYaml()
				if !ok {
					if test.redpandaYaml != "" {
						t.Error("missing actual redpanda yaml")
					}
				} else {
					actPath := "/actual/redpanda.yaml"
					act.WriteAt(fs, actPath)
					m[actPath] = testfs.RFile(test.redpandaYaml)
				}
			}
			{
				mat := cfg.VirtualRpkYaml()
				mat.Write(fs)
				m[defaultRpkPath] = testfs.RFile(test.expVirtualRpk)
			}
			{
				act, ok := cfg.ActualRpkYaml()
				if !ok {
					if test.rpkYaml != "" {
						t.Error("missing actual rpk yaml")
					}
				} else {
					actPath := "/actual/rpk.yaml"
					act.WriteAt(fs, actPath)
					m[actPath] = testfs.RFile(test.rpkYaml)
				}
			}

			testfs.ExpectExact(t, fs, m)
		})
	}
}

func TestConfig_parseDevOverrides(t *testing.T) {
	var c Config
	defer func() {
		if x := recover(); x != nil {
			t.Fatal(x)
		}
	}()
	c.parseDevOverrides()
}

func TestParamsHelpComplete(t *testing.T) {
	h := ParamsHelp()
	m := maps.Clone(xflags)
	for _, line := range strings.Split(h, "\n") {
		key := strings.Split(line, "=")[0]
		delete(m, key)
	}
	if len(m) > 0 {
		t.Errorf("ParamsHelp missing keys: %v", maps.Keys(m))
	}
}

func TestParamsListComplete(t *testing.T) {
	h := ParamsList()
	m := maps.Clone(xflags)
	for _, line := range strings.Split(h, "\n") {
		key := strings.Split(line, "=")[0]
		delete(m, key)
	}
	if len(m) > 0 {
		t.Errorf("ParamsList missing keys: %v", maps.Keys(m))
	}
}

func TestXSetExamples(t *testing.T) {
	m := maps.Clone(xflags)
	for _, fn := range []func() (xs, yamlPaths []string){
		XProfileFlags,
		XCloudAuthFlags,
		XRpkGlobalFlags,
	} {
		xs, yamlPaths := fn()
		for i, x := range xs {
			delete(m, x)

			xf := xflags[x]
			fs := afero.NewMemMapFs()
			cfg, _ := new(Params).Load(fs)
			y := cfg.VirtualRpkYaml()
			if err := xf.parse(xf.testExample, y); err != nil {
				t.Errorf("unable to parse test example for xflag %s: %v", x, err)
			}
			yamlPath := yamlPaths[i]
			var err error
			switch xf.kind {
			case xkindProfile:
				err = Set(new(RpkProfile), yamlPath, xf.testExample)
			case xkindCloudAuth:
				err = Set(new(RpkCloudAuth), yamlPath, xf.testExample)
			case xkindGlobal:
				err = Set(new(RpkYaml), yamlPath, xf.testExample)
			default:
				t.Errorf("unrecognized xflag kind %v", xf.kind)
				continue
			}
			if err != nil {
				t.Errorf("unable to Set test example for xflag yaml path %s: %v", yamlPath, err)
			}
		}
	}

	if len(m) > 0 {
		t.Errorf("xflags still contains keys %v after checking all examples in this test", maps.Keys(m))
	}
}

func TestXSetDefaultsPaths(t *testing.T) {
	xs, paths := XRpkGlobalFlags()
	for i, x := range xs {
		if paths[i] != x {
			t.Errorf("XRpkGlobalFlags() returned different xflag %s and path %s", x, paths[i])
		}
		if !strings.HasPrefix(x, "globals.") {
			t.Errorf("XRpkGlobalFlags() returned xflag %s that doesn't start with globals.", x)
		}
	}
}
