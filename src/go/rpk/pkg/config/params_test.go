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
    schema_registry:
        addresses:
            - 127.0.0.1:8081
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
				c.Rpk.SR.Addresses = []string{"200.4.2.1:8081"}
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
    schema_registry:
        addresses:
            - 200.4.2.1:8081
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
    schema_registry:
         addresses:
             - 127.0.0.1:8081
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
    schema_registry:
         addresses:
             - 127.0.0.1:8081
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
    schema_registry:
         addresses:
             - 127.0.0.1:8081
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
    schema_registry:
        addresses:
            - 127.0.0.1:8081
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{"baz:8082"},
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{"baz:8082"},
					},
				},
			},
		},
		{
			name: "kafka broker, admin api and SR from redpanda.yaml",
			inCfg: &RedpandaYaml{
				Redpanda: RedpandaNodeConfig{
					KafkaAPI: []NamedAuthNSocketAddress{
						{Address: "250.12.12.12", Port: 9095},
					},
					AdminAPI: []NamedSocketAddress{
						{Address: "0.0.2.3", Port: 4444},
					},
				},
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "9.12.10.1", Port: 5522},
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "9.12.10.1", Port: 5522},
					},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{"250.12.12.12:9095"},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{"0.0.2.3:4444"},
					},
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{"9.12.10.1:5522"},
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 2222, Name: "tls"}, // same as above, numbers in addr/port slightly changed
						{Address: "127.1.0.1", Port: 2222, Name: "tls"},
						{Address: "localhost", Port: 2222, Name: "tls"},
						{Address: "122.61.33.12", Port: 2222, Name: "tls"},
						{Address: "10.1.2.1", Port: 8888},
						{Address: "127.1.2.1", Port: 8888},
						{Address: "localhost", Port: 8888},
						{Address: "122.61.32.12", Port: 8888},
						{Address: "0.0.0.0", Port: 8888},
					},
					SchemaRegistryAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 2222, Name: "tls"},
						{Address: "127.1.0.1", Port: 2222, Name: "tls"},
						{Address: "localhost", Port: 2222, Name: "tls"},
						{Address: "122.61.33.12", Port: 2222, Name: "tls"},
						{Address: "10.1.2.1", Port: 8888},
						{Address: "127.1.2.1", Port: 8888},
						{Address: "localhost", Port: 8888},
						{Address: "122.61.32.12", Port: 8888},
						{Address: "0.0.0.0", Port: 8888},
					},
					SchemaRegistryAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{
							"localhost:8888",
							"127.1.2.1:8888",
							"127.0.0.1:8888",
							"10.1.2.1:8888",
							"122.61.32.12:8888",
						},
					},
				},
			},
		},
		{
			name: "broker, admin api, and SR sorted with TLS and MTLS",
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 3333, Name: "mtls"},
						{Address: "127.1.0.1", Port: 3333, Name: "mtls"},
						{Address: "localhost", Port: 3333, Name: "mtls"},
						{Address: "122.61.33.12", Port: 3333, Name: "mtls"},
						{Address: "10.1.0.1", Port: 8888, Name: "tls"},
						{Address: "127.1.0.1", Port: 8888, Name: "tls"},
						{Address: "localhost", Port: 8888, Name: "tls"},
						{Address: "122.61.33.12", Port: 8888, Name: "tls"},
					},
					SchemaRegistryAPITLS: []ServerTLS{
						{Name: "tls", Enabled: true},
						{Name: "mtls", Enabled: true, RequireClientAuth: true},
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 3333, Name: "mtls"},
						{Address: "127.1.0.1", Port: 3333, Name: "mtls"},
						{Address: "localhost", Port: 3333, Name: "mtls"},
						{Address: "122.61.33.12", Port: 3333, Name: "mtls"},
						{Address: "10.1.0.1", Port: 8888, Name: "tls"},
						{Address: "127.1.0.1", Port: 8888, Name: "tls"},
						{Address: "localhost", Port: 8888, Name: "tls"},
						{Address: "122.61.33.12", Port: 8888, Name: "tls"},
					},
					SchemaRegistryAPITLS: []ServerTLS{
						{Name: "tls", Enabled: true},
						{Name: "mtls", Enabled: true, RequireClientAuth: true},
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{
							"localhost:8888",
							"127.1.0.1:8888",
							"10.1.0.1:8888",
							"122.61.33.12:8888",
						},
					},
				},
			},
		},
		{
			name: "broker, admin api, and SR sorted with TLS",
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 7777, Name: "tls"},
						{Address: "127.1.0.1", Port: 7777, Name: "tls"},
						{Address: "localhost", Port: 7777, Name: "tls"},
						{Address: "122.61.33.12", Port: 7777, Name: "tls"},
					},
					SchemaRegistryAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
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
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "10.1.0.1", Port: 7777, Name: "tls"},
						{Address: "127.1.0.1", Port: 7777, Name: "tls"},
						{Address: "localhost", Port: 7777, Name: "tls"},
						{Address: "122.61.33.12", Port: 7777, Name: "tls"},
					},
					SchemaRegistryAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{
							"localhost:7777",
							"127.1.0.1:7777",
							"10.1.0.1:7777",
							"122.61.33.12:7777",
						},
					},
				},
			},
		},
		{
			name: "assume the admin API and SR when only Kafka API is available",
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{
							"127.1.0.1:8081",
						},
					},
				},
			},
		},
		{
			name: "assume the admin API and Kafka API when only schema registry is available",
			inCfg: &RedpandaYaml{
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "localhost", Port: 8888},
						{Address: "127.0.0.1", Port: 8888},
					},
					SchemaRegistryAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
			},
			expCfg: &RedpandaYaml{
				SchemaRegistry: &SchemaRegistry{
					SchemaRegistryAPI: []NamedAuthNSocketAddress{
						{Address: "localhost", Port: 8888},
						{Address: "127.0.0.1", Port: 8888},
					},
					SchemaRegistryAPITLS: []ServerTLS{{Name: "tls", Enabled: true}},
				},
				Rpk: RpkNodeConfig{
					KafkaAPI: RpkKafkaAPI{
						Brokers: []string{
							"localhost:9092", // we only use the first one to infer.
						},
					},
					AdminAPI: RpkAdminAPI{
						Addresses: []string{
							"localhost:9644",
						},
					},
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{
							"localhost:8888",
							"127.0.0.1:8888",
						},
					},
				},
			},
		},
		{
			name: "assume the Kafka API API and SR when only admin API is available from rpk with TLS",
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
					SR: RpkSchemaRegistryAPI{
						Addresses: []string{"127.1.0.1:8081"},
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
    schema_registry:
        addresses:
            - 127.0.0.1:8081
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
			expVirtualRpk: `version: 7
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
    schema_registry:
        addresses:
            - 127.0.0.1:3232
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
    schema_registry:
        addresses:
            - 127.0.0.1:3232
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,
			expVirtualRpk: `version: 7
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
            - 127.0.0.1:3232
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
			rpkYaml: `version: 7
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
    schema_registry:
        addresses:
            - 0.0.0.2:8081
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
			expVirtualRpk: `version: 7
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
    schema_registry:
        addresses:
            - 127.0.0.1:8081
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,
			rpkYaml: `version: 7
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
      schema_registry:
        addresses:
            - 127.0.0.1:3232
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
    schema_registry:
        addresses:
            - 127.0.0.1:3232
    enable_memory_locking: true
    tune_network: true
    tune_disk_scheduler: true
    tune_disk_nomerges: true
    tune_disk_write_cache: true
    tune_disk_irq: true
`,

			expVirtualRpk: `version: 7
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
            - 127.0.0.1:3232
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
	delete(m, xCloudEnvironment) // We leave this out of the list and docs on purpose.
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
	delete(m, xCloudEnvironment) // We leave this out of the list and docs on purpose.
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

func TestConfig_fixSchemePorts(t *testing.T) {
	tests := []struct {
		name   string
		config Config
		expect Config
		errMsg string
	}{
		{
			name: "fix missing ports in redpanda.yaml addresses",
			config: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{"broker1", "broker2:9093"},
						},
						AdminAPI: RpkAdminAPI{
							Addresses: []string{"address1", "address2:2222"},
						},
						SR: RpkSchemaRegistryAPI{
							Addresses: []string{"address1", "address2", "address3:8088"},
						},
					},
				},
			},
			expect: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{"broker1:9092", "broker2:9093"},
						},
						AdminAPI: RpkAdminAPI{
							Addresses: []string{"address1:9644", "address2:2222"},
						},
						SR: RpkSchemaRegistryAPI{
							Addresses: []string{"address1:8081", "address2:8081", "address3:8088"},
						},
					},
				},
			},
		},
		{
			name: "fix missing ports in profile brokers",
			config: Config{
				rpkYaml: RpkYaml{
					CurrentProfile: "default",
					Profiles: []RpkProfile{
						{
							Name: "default",
							KafkaAPI: RpkKafkaAPI{
								Brokers: []string{"profile-broker1", "profile-broker2:9094"},
							},
							AdminAPI: RpkAdminAPI{
								Addresses: []string{"address1", "address2:2222"},
							},
							SR: RpkSchemaRegistryAPI{
								Addresses: []string{"address1", "address2", "address3:8088"},
							},
						},
					},
				},
			},
			expect: Config{
				rpkYaml: RpkYaml{
					CurrentProfile: "default",
					Profiles: []RpkProfile{
						{
							Name: "default",
							KafkaAPI: RpkKafkaAPI{
								Brokers: []string{"profile-broker1:9092", "profile-broker2:9094"},
							},
							AdminAPI: RpkAdminAPI{
								Addresses: []string{"address1:9644", "address2:2222"},
							},
							SR: RpkSchemaRegistryAPI{
								Addresses: []string{"address1:8081", "address2:8081", "address3:8088"},
							},
						},
					},
				},
			},
		},
		{
			name: "fix missing ports in redpanda.yaml addresses",
			config: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{"broker1", "broker2:9093"},
						},
						AdminAPI: RpkAdminAPI{
							Addresses: []string{"address1", "address2:2222"},
						},
						SR: RpkSchemaRegistryAPI{
							Addresses: []string{"address1", "address2", "address3:8088"},
						},
					},
				},
			},
			expect: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{"broker1:9092", "broker2:9093"},
						},
						AdminAPI: RpkAdminAPI{
							Addresses: []string{"address1:9644", "address2:2222"},
						},
						SR: RpkSchemaRegistryAPI{
							Addresses: []string{"address1:8081", "address2:8081", "address3:8088"},
						},
					},
				},
			},
		},
		{
			name: "fix missing ports in redpanda.yaml addresses with IPv6",
			config: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{"[2001:db8::1]", "[2001:db8::2]:9093"},
						},
						AdminAPI: RpkAdminAPI{
							Addresses: []string{"[2001:db8::3]", "[2001:db8::4]:2222"},
						},
						SR: RpkSchemaRegistryAPI{
							Addresses: []string{"[2001:db8::5]", "[2001:db8::6]", "[2001:db8::7]:8088"},
						},
					},
				},
			},
			expect: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{"[2001:db8::1]:9092", "[2001:db8::2]:9093"},
						},
						AdminAPI: RpkAdminAPI{
							Addresses: []string{"[2001:db8::3]:9644", "[2001:db8::4]:2222"},
						},
						SR: RpkSchemaRegistryAPI{
							Addresses: []string{"[2001:db8::5]:8081", "[2001:db8::6]:8081", "[2001:db8::7]:8088"},
						},
					},
				},
			},
		},
		{
			name: "invalid broker address",
			config: Config{
				redpandaYaml: RedpandaYaml{
					Rpk: RpkNodeConfig{
						KafkaAPI: RpkKafkaAPI{
							Brokers: []string{":invalid"},
						},
					},
				},
			},
			expect: Config{},
			errMsg: "unable to fix broker address",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := tt.config.fixSchemePorts()
			if tt.errMsg != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.errMsg)
			} else {
				require.NoError(t, err)
				require.Equal(t, tt.expect, tt.config)
			}
		})
	}
}

func TestProfileEnvVar(t *testing.T) {
	for _, tt := range []struct {
		name        string
		rpkYaml     string
		envProfile  string
		flagProfile string
		expProfile  string
		expErr      string
	}{
		{
			name: "env var sets current profile",
			rpkYaml: `version: 7
current_profile: default
profiles:
    - name: default
      kafka_api:
        brokers:
            - 127.0.0.1:9092
    - name: myprofile
      kafka_api:
        brokers:
            - 192.168.1.1:9092
`,
			envProfile: "myprofile",
			expProfile: "myprofile",
		},
		{
			name: "flag takes precedence over env var",
			rpkYaml: `version: 7
current_profile: default
profiles:
    - name: default
      kafka_api:
        brokers:
            - 127.0.0.1:9092
    - name: envprofile
      kafka_api:
        brokers:
            - 192.168.1.1:9092
    - name: flagprofile
      kafka_api:
        brokers:
            - 10.0.0.1:9092
`,
			envProfile:  "envprofile",
			flagProfile: "flagprofile",
			expProfile:  "flagprofile",
		},
		{
			name: "error when env profile does not exist",
			rpkYaml: `version: 7
current_profile: default
profiles:
    - name: default
      kafka_api:
        brokers:
            - 127.0.0.1:9092
`,
			envProfile: "nonexistent",
			expErr:     `selected profile "nonexistent" does not exist`,
		},
		{
			name: "error when flag profile does not exist",
			rpkYaml: `version: 7
current_profile: default
profiles:
    - name: default
      kafka_api:
        brokers:
            - 127.0.0.1:9092
`,
			flagProfile: "nonexistent",
			expErr:      `selected profile "nonexistent" does not exist`,
		},
		{
			name: "no env var uses saved current profile",
			rpkYaml: `version: 7
current_profile: saved
profiles:
    - name: default
      kafka_api:
        brokers:
            - 127.0.0.1:9092
    - name: saved
      kafka_api:
        brokers:
            - 192.168.1.1:9092
`,
			expProfile: "saved",
		},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if tt.envProfile != "" {
				t.Setenv(ProfileEnvVar, tt.envProfile)
			}

			defaultRpkPath := "/rpk/rpk.yaml"
			m := make(map[string]testfs.Fmode)
			m[defaultRpkPath] = testfs.RFile(tt.rpkYaml)
			fs := testfs.FromMap(m)

			p := &Params{
				Profile:    tt.flagProfile,
				ConfigFlag: defaultRpkPath,
			}
			cfg, err := p.Load(fs)

			if tt.expErr != "" {
				require.Error(t, err)
				require.Contains(t, err.Error(), tt.expErr)
				return
			}
			require.NoError(t, err)

			y := cfg.VirtualRpkYaml()
			require.Equal(t, tt.expProfile, y.CurrentProfile)
		})
	}
}

func TestProcessOverrides(t *testing.T) {
	tests := []struct {
		name          string
		envOverrides  map[string]string
		flagOverrides []string
		applyCfg      func(*Config)
		expErr        string
		verify        func(t *testing.T, y *RpkYaml)
	}{
		{
			name: "Apply flag override to kafka brokers",
			flagOverrides: []string{
				"brokers=127.0.0.1:9092,localhost:9092",
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				want := []string{"127.0.0.1:9092", "localhost:9092"}
				require.Equal(t, want, p.KafkaAPI.Brokers)
			},
		},
		{
			name: "Apply env override to admin addresses",
			envOverrides: map[string]string{
				"RPK_ADMIN_HOSTS": "admin.redpanda.com,admin2.redpanda.com",
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				want := []string{"admin.redpanda.com", "admin2.redpanda.com"}
				require.Equal(t, want, p.AdminAPI.Addresses)
			},
		},
		{
			name: "Flag override takes precedence over env and cfg",
			envOverrides: map[string]string{
				"RPK_BROKERS": "from.env:9092",
			},
			flagOverrides: []string{
				"brokers=from.flag:9092",
			},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.KafkaAPI.Brokers = []string{"from.cfg:9092"}
			},
			verify: func(t *testing.T, y *RpkYaml) {
				want := []string{"from.flag:9092"}
				require.Equal(t, want, y.Profile(y.CurrentProfile).KafkaAPI.Brokers)
			},
		},
		{
			name:          "Apply tls enabled to kafka brokers",
			flagOverrides: []string{"tls.enabled=true"},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.True(t, p.KafkaAPI.TLS != nil)
			},
		},
		{
			name: "Apply kafka TLS enabled via env",
			envOverrides: map[string]string{
				"RPK_TLS_ENABLED": "true",
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.NotNil(t, p.KafkaAPI.TLS)
			},
		},
		{
			name:          "Apply tls.enabled=false to kafka brokers",
			flagOverrides: []string{"tls.enabled=false"},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.KafkaAPI.TLS = new(TLS)
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.Nil(t, p.KafkaAPI.TLS)
			},
		},
		{
			name: "Apply kafka TLS disabled via env",
			envOverrides: map[string]string{
				"RPK_TLS_ENABLED": "false",
			},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.KafkaAPI.TLS = new(TLS)
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.Nil(t, p.KafkaAPI.TLS)
			},
		},
		{
			name:          "Apply tls enabled to admin hosts",
			flagOverrides: []string{"admin.tls.enabled=true"},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.True(t, p.AdminAPI.TLS != nil)
			},
		},
		{
			name: "Apply admin TLS enabled via env",
			envOverrides: map[string]string{
				"RPK_ADMIN_TLS_ENABLED": "true",
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.NotNil(t, p.AdminAPI.TLS)
			},
		},
		{
			name:          "Apply tls.enabled=false to admin hosts",
			flagOverrides: []string{"admin.tls.enabled=false"},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.AdminAPI.TLS = new(TLS)
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.Nil(t, p.AdminAPI.TLS)
			},
		},
		{
			name: "Apply admin TLS disabled via env",
			envOverrides: map[string]string{
				"RPK_ADMIN_TLS_ENABLED": "false",
			},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.AdminAPI.TLS = new(TLS)
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.Nil(t, p.AdminAPI.TLS)
			},
		},
		{
			name:          "Apply tls enabled to SR hosts",
			flagOverrides: []string{"registry.tls.enabled=true"},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.True(t, p.SR.TLS != nil)
			},
		},
		{
			name: "Apply schema registry TLS enabled via env",
			envOverrides: map[string]string{
				"RPK_REGISTRY_TLS_ENABLED": "true",
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.NotNil(t, p.SR.TLS)
			},
		},
		{
			name:          "Apply tls.enabled=false to SR hosts",
			flagOverrides: []string{"registry.tls.enabled=false"},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.SR.TLS = new(TLS)
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.Nil(t, p.SR.TLS)
			},
		},
		{
			name: "Apply schema registry TLS disabled via env",
			envOverrides: map[string]string{
				"RPK_REGISTRY_TLS_ENABLED": "false",
			},
			applyCfg: func(cfg *Config) {
				p := cfg.rpkYaml.Profile(cfg.rpkYaml.CurrentProfile)
				p.SR.TLS = new(TLS)
			},
			verify: func(t *testing.T, y *RpkYaml) {
				p := y.Profile(y.CurrentProfile)
				require.Nil(t, p.SR.TLS)
			},
		},
		{
			name: "Error - invalid key format (missing =val)",
			flagOverrides: []string{
				"kafka_api.brokers",
			},
			expErr: `flag config: "kafka_api.brokers" is not a key=value`,
		},
		{
			name: "Error - unknown key",
			flagOverrides: []string{
				"unknown.key=value",
			},
			expErr: `flag config: unknown key "unknown.key"`,
		},
		{
			name: "Error - parse failure",
			flagOverrides: []string{
				"tls.insecure_skip_verify=notabool",
			},
			expErr: `flag config key "tls.insecure_skip_verify": strconv.ParseBool: parsing "notabool": invalid syntax`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			c := &Config{
				rpkYaml: RpkYaml{
					CurrentProfile: "default",
					Profiles:       []RpkProfile{{Name: "default"}},
				},
			}
			if tt.applyCfg != nil {
				tt.applyCfg(c)
			}
			p := &Params{
				FlagOverrides: tt.flagOverrides,
			}
			if tt.envOverrides != nil {
				for k, v := range tt.envOverrides {
					t.Setenv(k, v)
				}
			}
			err := p.processOverrides(c)
			if tt.expErr != "" {
				require.ErrorContains(t, err, tt.expErr, "expected error containing %q, got %v", tt.expErr, err)
				return
			}
			require.NoError(t, err)
			if tt.verify != nil {
				tt.verify(t, &c.rpkYaml)
			}
		})
	}
}

func TestIgnoreProfile(t *testing.T) {
	defaultRpkPath, err := DefaultRpkYamlPath()
	require.NoError(t, err)

	t.Run("ignores rpk.yaml and redpanda.yaml, uses defaults", func(t *testing.T) {
		// Set up files that would normally be loaded
		fs := testfs.FromMap(map[string]testfs.Fmode{
			defaultRpkPath: testfs.RFile(`version: 7
current_profile: custom
profiles:
    - name: custom
      kafka_api:
        brokers:
            - remote-host:9092
`),
			DefaultRedpandaYamlPath: testfs.RFile(`redpanda:
    kafka_api:
        - address: 10.0.0.1
          port: 9092
`),
		})

		p := &Params{IgnoreProfile: true}
		cfg, err := p.Load(fs)
		require.NoError(t, err)

		// Should use default localhost settings, not the config file values.
		profile := cfg.VirtualProfile()
		require.Equal(t, []string{"127.0.0.1:9092"}, profile.KafkaAPI.Brokers)
		require.Equal(t, []string{"127.0.0.1:9644"}, profile.AdminAPI.Addresses)
		require.Equal(t, []string{"127.0.0.1:8081"}, profile.SR.Addresses)
	})

	t.Run("allows environment variable overrides", func(t *testing.T) {
		fs := testfs.FromMap(map[string]testfs.Fmode{
			defaultRpkPath: testfs.RFile(`version: 7
current_profile: custom
profiles:
    - name: custom
      kafka_api:
        brokers:
            - remote-host:9092
`),
		})

		t.Setenv("RPK_BROKERS", "env-host:9092")

		p := &Params{IgnoreProfile: true}
		cfg, err := p.Load(fs)
		require.NoError(t, err)

		// Should use the env var override, not defaults or config file values.
		profile := cfg.VirtualProfile()
		require.Equal(t, []string{"env-host:9092"}, profile.KafkaAPI.Brokers)
	})

	t.Run("allows -X overrides", func(t *testing.T) {
		fs := testfs.FromMap(map[string]testfs.Fmode{
			defaultRpkPath: testfs.RFile(`version: 7
current_profile: custom
profiles:
    - name: custom
      kafka_api:
        brokers:
            - remote-host:9092
`),
		})

		p := &Params{
			IgnoreProfile: true,
			FlagOverrides: []string{"brokers=override-host:9092"},
		}
		cfg, err := p.Load(fs)
		require.NoError(t, err)

		// Should use the override value, not defaults or config file values.
		profile := cfg.VirtualProfile()
		require.Equal(t, []string{"override-host:9092"}, profile.KafkaAPI.Brokers)
	})

	t.Run("don't ignore rpk.yaml and redpanda.yaml", func(t *testing.T) {
		fs := testfs.FromMap(map[string]testfs.Fmode{
			defaultRpkPath: testfs.RFile(`version: 7
current_profile: custom
profiles:
    - name: custom
      kafka_api:
        brokers:
            - remote-host:9092
`),
			DefaultRedpandaYamlPath: testfs.RFile(`redpanda:
    kafka_api:
        - address: 10.0.0.1
          port: 9092
rpk:
  kafka_api:
    brokers: 100.100.100.1:9092
  admin_api:
    addresses: from-redpanda:9644
      
`),
		})

		p := &Params{IgnoreProfile: false} // The default
		cfg, err := p.Load(fs)
		require.NoError(t, err)

		profile := cfg.VirtualProfile()
		// Should use what's in the profile (not in the redpanda.yaml).
		require.Equal(t, []string{"remote-host:9092"}, profile.KafkaAPI.Brokers)
		// Should use what's in the redpanda.yaml as the profile doesn't have anything.
		require.Equal(t, []string{"from-redpanda:9644"}, profile.AdminAPI.Addresses)
		// Should 'guess' the host from kafka's host in the profile. SR is not
		// set anywhere.
		require.Equal(t, []string{"remote-host:8081"}, profile.SR.Addresses)
	})
}
