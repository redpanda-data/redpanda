// Copyright 2020 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"fmt"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func getValidConfig() *Config {
	conf := Default()
	conf.Redpanda.SeedServers = []SeedServer{
		{
			SocketAddress{"127.0.0.1", 33145},
		},
		{
			SocketAddress{"127.0.0.1", 33146},
		},
	}
	conf.Redpanda.DeveloperMode = false
	conf.Rpk = RpkConfig{
		EnableUsageStats:         true,
		TuneNetwork:              true,
		TuneDiskScheduler:        true,
		TuneDiskWriteCache:       true,
		TuneNomerges:             true,
		TuneDiskIrq:              true,
		TuneFstrim:               true,
		TuneCPU:                  true,
		TuneAioEvents:            true,
		TuneClocksource:          true,
		TuneSwappiness:           true,
		TuneTransparentHugePages: true,
		EnableMemoryLocking:      true,
		TuneCoredump:             true,
		TuneBallastFile:          true,
		CoredumpDir:              "/var/lib/redpanda/coredumps",
		WellKnownIo:              "vendor:vm:storage",
	}
	return conf
}

func TestSet(t *testing.T) {
	tests := []struct {
		name      string
		key       string
		value     string
		format    string
		check     func(st *testing.T, c *Config)
		expectErr bool
	}{
		{
			name:   "it should parse '1' as an int and not as bool (true)",
			key:    "redpanda.node_id",
			value:  "1",
			format: "single",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, 1, c.Redpanda.ID)
			},
		},
		{
			name:   "it should set single integer fields",
			key:    "redpanda.node_id",
			value:  "54312",
			format: "single",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, 54312, c.Redpanda.ID)
			},
		},
		{
			name:  "it should detect single integer fields if format isn't passed",
			key:   "redpanda.node_id",
			value: "54312",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, 54312, c.Redpanda.ID)
			},
		},
		{
			name:   "it should set single float fields",
			key:    "redpanda.float_field",
			value:  "42.3",
			format: "single",
			check: func(st *testing.T, cfg *Config) {
				require.Exactly(st, 42.3, cfg.Redpanda.Other["float_field"])
			},
		},
		{
			name:  "it should detect single float fields if format isn't passed",
			key:   "redpanda.float_field",
			value: "42.3",
			check: func(st *testing.T, cfg *Config) {
				require.Exactly(st, 42.3, cfg.Redpanda.Other["float_field"])
			},
		},
		{
			name:   "it should set single string fields",
			key:    "redpanda.data_directory",
			value:  "/var/lib/differentdir",
			format: "single",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, "/var/lib/differentdir", c.Redpanda.Directory)
			},
		},
		{
			name:  "it should detect single string fields if format isn't passed",
			key:   "redpanda.data_directory",
			value: "/var/lib/differentdir",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, "/var/lib/differentdir", c.Redpanda.Directory)
			},
		},
		{
			name:   "it should set single bool fields",
			key:    "rpk.enable_usage_stats",
			value:  "true",
			format: "single",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, true, c.Rpk.EnableUsageStats)
			},
		},
		{
			name:  "it should detect single bool fields if format isn't passed",
			key:   "rpk.enable_usage_stats",
			value: "true",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, true, c.Rpk.EnableUsageStats)
			},
		},
		{
			name: "it should partially set map fields (yaml)",
			key:  "rpk",
			value: `tune_disk_irq: true
tune_cpu: true`,
			format: "yaml",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, true, c.Rpk.TuneDiskIrq)
				require.Exactly(st, true, c.Rpk.TuneCPU)
			},
		},
		{
			name:  "it should detect pandaproxy client single field if format isn't passed",
			key:   "pandaproxy_client.retries",
			value: "42",
			check: func(st *testing.T, c *Config) {
				require.Exactly(st, 42, c.PandaproxyClient.Other["retries"])
			},
		},
		{
			name: "it should detect yaml-formatted values if format isn't passed",
			key:  "redpanda.kafka_api",
			value: `- name: external
  address: 192.168.73.45
  port: 9092
- name: internal
  address: 10.21.34.58
  port: 9092
`,
			check: func(st *testing.T, c *Config) {
				expected := []NamedSocketAddress{{
					Name:    "external",
					Address: "192.168.73.45",
					Port:    9092,
				}, {
					Name:    "internal",
					Address: "10.21.34.58",
					Port:    9092,
				}}
				require.Exactly(st, expected, c.Redpanda.KafkaAPI)
			},
		},
		{
			name: "it should partially set map fields (json)",
			key:  "redpanda.kafka_api",
			value: `[{
		  "address": "192.168.54.2",
		  "port": 9092
		}]`,
			format: "json",
			check: func(st *testing.T, c *Config) {
				expected := []NamedSocketAddress{{
					Port:    9092,
					Address: "192.168.54.2",
				}}
				require.Exactly(st, expected, c.Redpanda.KafkaAPI)
			},
		},
		{
			name: "it should detect json-formatted values if format isn't passed",
			key:  "redpanda.advertised_kafka_api",
			value: `[{
		  "address": "192.168.54.2",
		  "port": 9092
		}]`,
			check: func(st *testing.T, c *Config) {
				expected := []NamedSocketAddress{{
					Port:    9092,
					Address: "192.168.54.2",
				}}
				require.Exactly(st, expected, c.Redpanda.AdvertisedKafkaAPI)
			},
		},
		{
			name:      "it should fail if the value isn't well formatted (json)",
			key:       "redpanda",
			value:     `{"seed_servers": []`,
			format:    "json",
			expectErr: true,
		},
		{
			name: "it should fail if the value isn't well formatted (yaml)",
			key:  "redpanda",
			value: `seed_servers:
		- host:
		  address: "123.`,
			format:    "yaml",
			expectErr: true,
		},
		{
			name:      "it should fail if the format isn't supported",
			key:       "redpanda",
			value:     "node_id=1",
			format:    "toml",
			expectErr: true,
		},
		{
			name:      "it should fail if no key is passed",
			value:     `node_id=1`,
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			cfg, err := new(Params).Load(fs)
			require.NoError(t, err)
			err = cfg.Set(tt.key, tt.value, tt.format)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, cfg)
			}
		})
	}
}

func TestDefault(t *testing.T) {
	defaultConfig := Default()
	expected := &Config{
		ConfigFile:     "/etc/redpanda/redpanda.yaml",
		Pandaproxy:     &Pandaproxy{},
		SchemaRegistry: &SchemaRegistry{},
		Redpanda: RedpandaConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{"0.0.0.0", 33145},
			KafkaAPI: []NamedSocketAddress{{
				Address: "0.0.0.0",
				Port:    9092,
			}},
			AdminAPI: []NamedSocketAddress{{
				Address: "0.0.0.0",
				Port:    9644,
			}},
			ID:            0,
			SeedServers:   []SeedServer{},
			DeveloperMode: true,
		},
		Rpk: RpkConfig{
			CoredumpDir: "/var/lib/redpanda/coredump",
		},
	}
	require.Exactly(t, expected, defaultConfig)
}

func TestWrite(t *testing.T) {
	tests := []struct {
		name         string
		existingConf string
		conf         func() *Config
		wantErr      bool
		expected     string
	}{
		{
			name: "write default values",
			conf: getValidConfig,
			expected: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 0
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
  rpc_server:
    address: 0.0.0.0
    port: 33145
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  admin:
  - address: 0.0.0.0
    port: 9644
  developer_mode: false
rpk:
  enable_usage_stats: true
  tune_network: true
  tune_disk_scheduler: true
  tune_disk_nomerges: true
  tune_disk_write_cache: true
  tune_disk_irq: true
  tune_fstrim: true
  tune_cpu: true
  tune_aio_events: true
  tune_clocksource: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  enable_memory_locking: true
  tune_coredump: true
  coredump_dir: /var/lib/redpanda/coredumps
  tune_ballast_file: true
  well_known_io: vendor:vm:storage
  overprovisioned: false
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "write additional values",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.AdvertisedRPCAPI = &SocketAddress{
					"174.32.64.2",
					33145,
				}
				return c
			},
			expected: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 0
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
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
    address: 174.32.64.2
    port: 33145
  developer_mode: false
rpk:
  enable_usage_stats: true
  tune_network: true
  tune_disk_scheduler: true
  tune_disk_nomerges: true
  tune_disk_write_cache: true
  tune_disk_irq: true
  tune_fstrim: true
  tune_cpu: true
  tune_aio_events: true
  tune_clocksource: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  enable_memory_locking: true
  tune_coredump: true
  coredump_dir: /var/lib/redpanda/coredumps
  tune_ballast_file: true
  well_known_io: vendor:vm:storage
  overprovisioned: false
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "write if empty struct is passed",
			conf: func() *Config {
				c := getValidConfig()
				c.Rpk = RpkConfig{}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 0
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
  rpc_server:
    address: 0.0.0.0
    port: 33145
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  admin:
  - address: 0.0.0.0
    port: 9644
  developer_mode: false
rpk:
  enable_usage_stats: false
  tune_network: false
  tune_disk_scheduler: false
  tune_disk_nomerges: false
  tune_disk_write_cache: false
  tune_disk_irq: false
  tune_fstrim: false
  tune_cpu: false
  tune_aio_events: false
  tune_clocksource: false
  tune_swappiness: false
  tune_transparent_hugepages: false
  enable_memory_locking: false
  tune_coredump: false
  tune_ballast_file: false
  overprovisioned: false
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "write unrecognized values ('Other' map).",
			conf: func() *Config {
				c := getValidConfig()
				size := 536870912
				if c.Redpanda.Other == nil {
					c.Redpanda.Other = make(map[string]interface{})
				}
				c.Redpanda.Other["log_segment_size"] = &size
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
redpanda:
  data_directory: /var/lib/redpanda/data
  node_id: 0
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
  rpc_server:
    address: 0.0.0.0
    port: 33145
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  admin:
  - address: 0.0.0.0
    port: 9644
  developer_mode: false
  log_segment_size: 536870912
rpk:
  enable_usage_stats: true
  tune_network: true
  tune_disk_scheduler: true
  tune_disk_nomerges: true
  tune_disk_write_cache: true
  tune_disk_irq: true
  tune_fstrim: true
  tune_cpu: true
  tune_aio_events: true
  tune_clocksource: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  enable_memory_locking: true
  tune_coredump: true
  coredump_dir: /var/lib/redpanda/coredumps
  tune_ballast_file: true
  well_known_io: vendor:vm:storage
  overprovisioned: false
pandaproxy: {}
schema_registry: {}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.conf().ConfigFile
			fs := afero.NewMemMapFs()
			if tt.existingConf != "" {
				_, err := utils.WriteBytes(fs, []byte(tt.existingConf), path)
				require.NoError(t, err)
			}

			err := tt.conf().Write(fs)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			contentBytes, err := afero.ReadFile(fs, path)
			require.NoError(t, err)
			content := string(contentBytes)
			fmt.Println(content)
			require.Equal(t, tt.expected, content)
		})
	}
}

func TestSetMode(t *testing.T) {
	fillRpkConfig := func(mode string) func() *Config {
		return func() *Config {
			conf := Default()
			val := mode == ModeProd
			conf.Redpanda.DeveloperMode = !val
			conf.Rpk = RpkConfig{
				TuneNetwork:        val,
				TuneDiskScheduler:  val,
				TuneNomerges:       val,
				TuneDiskWriteCache: val,
				TuneDiskIrq:        val,
				TuneFstrim:         false,
				TuneCPU:            val,
				TuneAioEvents:      val,
				TuneClocksource:    val,
				TuneSwappiness:     val,
				CoredumpDir:        conf.Rpk.CoredumpDir,
				Overprovisioned:    !val,
				TuneBallastFile:    val,
			}
			return conf
		}
	}

	tests := []struct {
		name           string
		mode           string
		startingConf   func() *Config
		expectedConfig func() *Config
		expectedErrMsg string
	}{
		{
			name:           "it should disable all tuners for dev mode",
			mode:           ModeDev,
			expectedConfig: fillRpkConfig(ModeDev),
		},
		{
			name:           "it should disable all tuners for dev mode ('development')",
			mode:           "development",
			expectedConfig: fillRpkConfig(ModeDev),
		},
		{
			name:           "it should disable all tuners for dev mode ('')",
			mode:           "",
			expectedConfig: fillRpkConfig(ModeDev),
		},
		{
			name:           "it should enable all the default tuners for prod mode",
			mode:           ModeProd,
			expectedConfig: fillRpkConfig(ModeProd),
		},
		{
			name:           "it should enable all the default tuners for prod mode ('production')",
			mode:           ModeProd,
			expectedConfig: fillRpkConfig(ModeProd),
		},
		{
			name:           "it should return an error for invalid modes",
			mode:           "winning",
			expectedErrMsg: "'winning' is not a supported mode. Available modes: dev, development, prod, production",
		},
		{
			name: "it should preserve all the values that shouldn't be reset",
			startingConf: func() *Config {
				conf := Default()
				conf.Rpk.AdminAPI = RpkAdminAPI{
					Addresses: []string{"some.addr.com:33145"},
				}
				conf.Rpk.KafkaAPI = RpkKafkaAPI{
					Brokers: []string{"192.168.76.54:9092"},
					TLS: &TLS{
						KeyFile:  "some-key.pem",
						CertFile: "some-cert.pem",
					},
				}
				conf.Rpk.AdditionalStartFlags = []string{"--memory=3G"}
				return conf
			},
			mode: ModeProd,
			expectedConfig: func() *Config {
				conf := fillRpkConfig(ModeProd)()
				conf.Rpk.AdminAPI = RpkAdminAPI{
					Addresses: []string{"some.addr.com:33145"},
				}
				conf.Rpk.KafkaAPI = RpkKafkaAPI{
					Brokers: []string{"192.168.76.54:9092"},
					TLS: &TLS{
						KeyFile:  "some-key.pem",
						CertFile: "some-cert.pem",
					},
				}
				conf.Rpk.AdditionalStartFlags = []string{"--memory=3G"}
				return conf
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			defaultConf := Default()
			if tt.startingConf != nil {
				defaultConf = tt.startingConf()
			}
			conf, err := SetMode(tt.mode, defaultConf)
			if tt.expectedErrMsg != "" {
				require.EqualError(t, err, tt.expectedErrMsg)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.expectedConfig(), conf)
		})
	}
}

func TestCheckConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     func() *Config
		expected []string
	}{
		{
			name:     "shall return no errors when config is valid",
			conf:     getValidConfig,
			expected: []string{},
		},
		{
			name: "shall return an error when config file does not contain data directory setting",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.Directory = ""
				return c
			},
			expected: []string{"redpanda.data_directory can't be empty"},
		},
		{
			name: "shall return an error when id of server is negative",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.ID = -100
				return c
			},
			expected: []string{"redpanda.node_id can't be a negative integer"},
		},
		{
			name: "shall return an error when the RPC server port is 0",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.RPCServer.Port = 0
				return c
			},
			expected: []string{"redpanda.rpc_server.port can't be 0"},
		},
		{
			name: "shall return an error when the RPC server address is empty",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.RPCServer.Address = ""
				return c
			},
			expected: []string{"redpanda.rpc_server.address can't be empty"},
		},
		{
			name: "shall return an error when the Kafka API port is 0",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.KafkaAPI[0].Port = 0
				return c
			},
			expected: []string{"redpanda.kafka_api.0.port can't be 0"},
		},
		{
			name: "shall return an error when the Kafka API address is empty",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.KafkaAPI[0].Address = ""
				return c
			},
			expected: []string{"redpanda.kafka_api.0.address can't be empty"},
		},
		{
			name: "shall return an error when one of the seed servers' address is empty",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.SeedServers[0].Host.Address = ""
				return c
			},
			expected: []string{"redpanda.seed_servers.0.host.address can't be empty"},
		},
		{
			name: "shall return an error when one of the seed servers' port is 0",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.SeedServers[1].Host.Port = 0
				return c
			},
			expected: []string{"redpanda.seed_servers.1.host.port can't be 0"},
		},
		{
			name: "shall return no errors when tune_coredump is set to false," +
				"regardless of coredump_dir's value",
			conf: func() *Config {
				c := getValidConfig()
				c.Rpk.TuneCoredump = false
				c.Rpk.CoredumpDir = ""
				return c
			},
			expected: []string{},
		},
		{
			name: "shall return an error when tune_coredump is set to true," +
				"but coredump_dir is empty",
			conf: func() *Config {
				c := getValidConfig()
				c.Rpk.CoredumpDir = ""
				return c
			},
			expected: []string{"if rpk.tune_coredump is set to true, rpk.coredump_dir can't be empty"},
		},
		{
			name: "shall return no error if setup is empty," +
				"but coredump_dir is empty",
			conf: func() *Config {
				c := getValidConfig()
				c.Rpk.WellKnownIo = ""
				return c
			},
			expected: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := Check(tt.conf())
			errMsgs := []string{}
			for _, err := range got {
				errMsgs = append(errMsgs, err.Error())
			}
			require.Exactly(t, tt.expected, errMsgs)
		})
	}
}
