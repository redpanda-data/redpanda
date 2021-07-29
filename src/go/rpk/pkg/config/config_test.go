// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package config

import (
	"path/filepath"
	"testing"

	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/utils"
	vyaml "github.com/vectorizedio/redpanda/src/go/rpk/pkg/yaml"
	"gopkg.in/yaml.v2"
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
		TuneCpu:                  true,
		TuneAioEvents:            true,
		TuneClocksource:          true,
		TuneSwappiness:           true,
		TuneTransparentHugePages: true,
		EnableMemoryLocking:      true,
		TuneCoredump:             true,
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
		check     func(st *testing.T, c *Config, mgr *manager)
		expectErr bool
	}{
		{
			name:   "it should parse '1' as an int and not as bool (true)",
			key:    "redpanda.node_id",
			value:  "1",
			format: "single",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, 1, c.Redpanda.Id)
			},
		},
		{
			name:   "it should set single integer fields",
			key:    "redpanda.node_id",
			value:  "54312",
			format: "single",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, 54312, c.Redpanda.Id)
			},
		},
		{
			name:  "it should detect single integer fields if format isn't passed",
			key:   "redpanda.node_id",
			value: "54312",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, 54312, c.Redpanda.Id)
			},
		},
		{
			name:   "it should set single float fields",
			key:    "redpanda.float_field",
			value:  "42.3",
			format: "single",
			check: func(st *testing.T, _ *Config, mgr *manager) {
				//require.True(st, ok, "Config map is of the wrong type")
				require.Exactly(st, 42.3, mgr.v.Get("redpanda.float_field"))
			},
		},
		{
			name:  "it should detect single float fields if format isn't passed",
			key:   "redpanda.float_field",
			value: "42.3",
			check: func(st *testing.T, _ *Config, mgr *manager) {
				//require.True(st, ok, "Config map is of the wrong type")
				require.Exactly(st, 42.3, mgr.v.Get("redpanda.float_field"))
			},
		},
		{
			name:   "it should set single string fields",
			key:    "redpanda.data_directory",
			value:  "'/var/lib/differentdir'",
			format: "single",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, "'/var/lib/differentdir'", c.Redpanda.Directory)
			},
		},
		{
			name:  "it should detect single string fields if format isn't passed",
			key:   "redpanda.data_directory",
			value: "/var/lib/differentdir",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, "/var/lib/differentdir", c.Redpanda.Directory)
			},
		},
		{
			name:   "it should set single bool fields",
			key:    "rpk.enable_usage_stats",
			value:  "true",
			format: "single",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, true, c.Rpk.EnableUsageStats)
			},
		},
		{
			name:  "it should detect single bool fields if format isn't passed",
			key:   "rpk.enable_usage_stats",
			value: "true",
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, true, c.Rpk.EnableUsageStats)
			},
		},
		{
			name:   "it should partially set map fields (yaml)",
			key:    "rpk",
			value:  `tune_disk_irq: true`,
			format: "yaml",
			check: func(st *testing.T, c *Config, _ *manager) {
				expected := RpkConfig{
					EnableUsageStats:         false,
					Overprovisioned:          false,
					TuneNetwork:              false,
					TuneDiskScheduler:        false,
					TuneNomerges:             false,
					TuneDiskIrq:              true,
					TuneCpu:                  false,
					TuneAioEvents:            false,
					TuneClocksource:          false,
					TuneSwappiness:           false,
					TuneTransparentHugePages: false,
					EnableMemoryLocking:      false,
					TuneFstrim:               false,
					TuneCoredump:             false,
					TuneDiskWriteCache:       false,
					CoredumpDir:              "/var/lib/redpanda/coredump",
				}
				require.Exactly(st, expected, c.Rpk)
			},
		},
		{
			name:  "it should detect pandaproxy client single field if format isn't passed",
			key:   "pandaproxy_client.retries",
			value: "42",
			check: func(st *testing.T, c *Config, mgr *manager) {
				require.Exactly(st, 42.0, c.PandaproxyClient.Other["retries"])
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
			check: func(st *testing.T, c *Config, _ *manager) {
				expected := []NamedSocketAddress{{
					Name: "external",
					SocketAddress: SocketAddress{
						Address: "192.168.73.45",
						Port:    9092,
					},
				}, {
					Name: "internal",
					SocketAddress: SocketAddress{
						Address: "10.21.34.58",
						Port:    9092,
					},
				}}
				require.Exactly(st, expected, c.Redpanda.KafkaApi)
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
			check: func(st *testing.T, c *Config, _ *manager) {
				expected := []NamedSocketAddress{{
					SocketAddress: SocketAddress{
						Port:    9092,
						Address: "192.168.54.2",
					},
				}}
				require.Exactly(st, expected, c.Redpanda.KafkaApi)
			},
		},
		{
			name: "it should detect json-formatted values if format isn't passed",
			key:  "redpanda.advertised_kafka_api",
			value: `[{
		  "address": "192.168.54.2",
		  "port": 9092
		}]`,
			check: func(st *testing.T, c *Config, _ *manager) {
				expected := []NamedSocketAddress{{
					SocketAddress: SocketAddress{
						Port:    9092,
						Address: "192.168.54.2",
					},
				}}
				require.Exactly(st, expected, c.Redpanda.AdvertisedKafkaApi)
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
			mgr := NewManager(fs)
			conf := Default()
			err := mgr.Set(tt.key, tt.value, tt.format)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				conf, err = mgr.Get()
				require.NoError(t, err)
				m, _ := mgr.(*manager)
				tt.check(t, conf, m)
			}
		})
	}
}

func TestMerge(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		check     func(st *testing.T, c *Config, mgr *manager)
		expectErr bool
	}{
		{
			name: "it should merge Kafka API spec",
			config: Config{
				Redpanda: RedpandaConfig{
					KafkaApi: []NamedSocketAddress{{
						Name: "kafka-api-name",
						SocketAddress: SocketAddress{
							"1.2.3.4",
							9123,
						},
					}},
				},
			},
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, "kafka-api-name", c.Redpanda.KafkaApi[0].Name)
				require.Exactly(st, "1.2.3.4", c.Redpanda.KafkaApi[0].Address)
				require.Exactly(st, 9123, c.Redpanda.KafkaApi[0].Port)
			},
		},
		{
			name: "it should merge Pandaproxy API spec",
			config: Config{
				Pandaproxy: &Pandaproxy{
					PandaproxyAPI: []NamedSocketAddress{{
						Name: "proxy-api-name",
						SocketAddress: SocketAddress{
							"1.2.3.4",
							8123,
						},
					}},
				},
			},
			check: func(st *testing.T, c *Config, _ *manager) {
				require.Exactly(st, "proxy-api-name", c.Pandaproxy.PandaproxyAPI[0].Name)
				require.Exactly(st, "1.2.3.4", c.Pandaproxy.PandaproxyAPI[0].Address)
				require.Exactly(st, 8123, c.Pandaproxy.PandaproxyAPI[0].Port)
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			mgr := NewManager(fs)
			err := mgr.Merge(&tt.config)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			if tt.check != nil {
				conf, err := mgr.Get()
				require.NoError(t, err)
				m, _ := mgr.(*manager)
				tt.check(t, conf, m)
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
			KafkaApi: []NamedSocketAddress{{
				SocketAddress: SocketAddress{
					"0.0.0.0",
					9092,
				},
			}},
			AdminApi: []NamedSocketAddress{{
				SocketAddress: SocketAddress{
					"0.0.0.0",
					9644,
				},
			}},
			Id:            0,
			SeedServers:   []SeedServer{},
			DeveloperMode: true,
		},
		Rpk: RpkConfig{
			CoredumpDir: "/var/lib/redpanda/coredump",
		},
	}
	require.Exactly(t, expected, defaultConfig)
}

func TestRead(t *testing.T) {
	const baseDir string = "/etc/redpanda"
	tests := []struct {
		name    string
		path    string
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
			path:    baseDir + "/redpanda.yaml",
			want:    getValidConfig,
			wantErr: false,
		},
		// TODO: Add tests for when the config file has missing objects
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			mgr := NewManager(fs)
			err := tt.before(fs, tt.path)
			require.NoError(t, err)
			got, err := mgr.Read(tt.path)
			want := tt.want()
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			require.True(t, filepath.IsAbs(got.ConfigFile))
			require.Exactly(t, want, got)
		})
	}
}

func TestWrite(t *testing.T) {
	const path string = "/redpanda.yaml"
	type args struct {
	}
	tests := []struct {
		name         string
		existingConf string
		conf         func() *Config
		wantErr      bool
		expected     string
	}{
		{
			name:    "it should write the default values for redpanda.kafka_api if it's null",
			conf:    getValidConfig,
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name:    "it should write the default values if redpanda.kafka_api is missing",
			conf:    getValidConfig,
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write a valid config file without advertised_kafka_api",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.AdvertisedRPCAPI = &SocketAddress{
					"174.32.64.2",
					33145,
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  advertised_rpc_api:
    address: 174.32.64.2
    port: 33145
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write a valid config file without advertised_rpc_api",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.AdvertisedKafkaApi = []NamedSocketAddress{{
					SocketAddress: SocketAddress{
						"174.32.64.2",
						9092,
					},
				}}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  advertised_kafka_api:
  - address: 174.32.64.2
    port: 9092
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write a valid config file without an rpk config object",
			conf: func() *Config {
				c := getValidConfig()
				c.Rpk = RpkConfig{}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredump
  enable_memory_locking: false
  enable_usage_stats: false
  overprovisioned: false
  tune_aio_events: false
  tune_clocksource: false
  tune_coredump: false
  tune_cpu: false
  tune_disk_irq: false
  tune_disk_nomerges: false
  tune_disk_scheduler: false
  tune_disk_write_cache: false
  tune_fstrim: false
  tune_network: false
  tune_swappiness: false
  tune_transparent_hugepages: false
schema_registry: {}
`,
		},
		{
			name: "shall fail with an invalid config",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.Directory = ""
				return c
			},
			wantErr: true,
		},
		{
			name:    "shall write a valid config file with an rpk config object",
			conf:    getValidConfig,
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall leave unrecognized fields untouched",
			existingConf: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_doc_dir: /usr/share/redpanda/admin-api-doc
  data_directory: /var/lib/redpanda/data
  default_window_sec: 100
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
    node_id: 1
  - host:
      address: 127.0.0.1
      port: 33146
    node_id: 2
  target_quota_byte_rate: 1000000
unrecognized_top_field:
  child: true
`,
			conf:    getValidConfig,
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_doc_dir: /usr/share/redpanda/admin-api-doc
  data_directory: /var/lib/redpanda/data
  default_window_sec: 100
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
  target_quota_byte_rate: 1000000
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
unrecognized_top_field:
  child: true
`,
		},
		{
			name: "should merge the new config onto the current one, not just overwrite it",
			existingConf: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_doc_dir: /usr/share/redpanda/admin-api-doc
  auto_create_topics_enabled: true
  data_directory: /var/lib/redpanda/data
  default_window_sec: 100
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  target_quota_byte_rate: 1000000
`,
			conf: func() *Config {
				conf := getValidConfig()
				conf.Redpanda.SeedServers = []SeedServer{}
				conf.Redpanda.Directory = "/different/path"
				return conf
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_doc_dir: /usr/share/redpanda/admin-api-doc
  auto_create_topics_enabled: true
  data_directory: /different/path
  default_window_sec: 100
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers: []
  target_quota_byte_rate: 1000000
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with tls configuration",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.KafkaApi[0].Name = "outside"
				c.Redpanda.KafkaApiTLS = []ServerTLS{{
					Name:              "outside",
					KeyFile:           "/etc/certs/cert.key",
					TruststoreFile:    "/etc/certs/ca.crt",
					CertFile:          "/etc/certs/cert.crt",
					Enabled:           true,
					RequireClientAuth: true,
				}}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    name: outside
    port: 9092
  kafka_api_tls:
  - cert_file: /etc/certs/cert.crt
    enabled: true
    key_file: /etc/certs/cert.key
    name: outside
    require_client_auth: true
    truststore_file: /etc/certs/ca.crt
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with admin tls configuration",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.AdminApiTLS = []ServerTLS{{
					KeyFile:           "/etc/certs/admin/cert.key",
					TruststoreFile:    "/etc/certs/admin/ca.crt",
					CertFile:          "/etc/certs/admin/cert.crt",
					Enabled:           true,
					RequireClientAuth: true,
				}}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_tls:
  - cert_file: /etc/certs/admin/cert.crt
    enabled: true
    key_file: /etc/certs/admin/cert.key
    require_client_auth: true
    truststore_file: /etc/certs/admin/ca.crt
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with log_segment_size configuration",
			conf: func() *Config {
				c := getValidConfig()
				size := 536870912
				c.Redpanda.LogSegmentSize = &size
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  log_segment_size: 536870912
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with full pandaproxy configuration",
			conf: func() *Config {
				c := getValidConfig()
				c.Pandaproxy = &Pandaproxy{
					PandaproxyAPI: []NamedSocketAddress{
						{
							Name: "first",
							SocketAddress: SocketAddress{
								Address: "1.2.3.4",
								Port:    1234,
							},
						},
					},
					PandaproxyAPITLS: []ServerTLS{{
						KeyFile:           "/etc/certs/cert.key",
						TruststoreFile:    "/etc/certs/ca.crt",
						CertFile:          "/etc/certs/cert.crt",
						Enabled:           true,
						RequireClientAuth: true,
					}},
					AdvertisedPandaproxyAPI: []NamedSocketAddress{
						{
							Name: "advertised",
							SocketAddress: SocketAddress{
								Address: "2.3.4.1",
								Port:    2341,
							},
						},
					},
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy:
  advertised_pandaproxy_api:
  - address: 2.3.4.1
    name: advertised
    port: 2341
  pandaproxy_api:
  - address: 1.2.3.4
    name: first
    port: 1234
  pandaproxy_api_tls:
  - cert_file: /etc/certs/cert.crt
    enabled: true
    key_file: /etc/certs/cert.key
    require_client_auth: true
    truststore_file: /etc/certs/ca.crt
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with pandaproxy api only configuration",
			conf: func() *Config {
				c := getValidConfig()
				c.Pandaproxy = &Pandaproxy{
					PandaproxyAPI: []NamedSocketAddress{
						{
							Name: "first",
							SocketAddress: SocketAddress{
								Address: "1.2.3.4",
								Port:    1234,
							},
						},
					},
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy:
  pandaproxy_api:
  - address: 1.2.3.4
    name: first
    port: 1234
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with pandaproxy client configuration",
			conf: func() *Config {
				c := getValidConfig()
				c.PandaproxyClient = &KafkaClient{
					Brokers: []SocketAddress{
						{
							Address: "1.2.3.4",
							Port:    1234,
						},
					},
					BrokerTLS: ServerTLS{
						KeyFile:           "/etc/certs/cert.key",
						TruststoreFile:    "/etc/certs/ca.crt",
						CertFile:          "/etc/certs/cert.crt",
						Enabled:           true,
						RequireClientAuth: true,
					},
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
pandaproxy_client:
  broker_tls:
    cert_file: /etc/certs/cert.crt
    enabled: true
    key_file: /etc/certs/cert.key
    require_client_auth: true
    truststore_file: /etc/certs/ca.crt
  brokers:
  - address: 1.2.3.4
    port: 1234
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with group_topic_partitions value",
			conf: func() *Config {
				c := getValidConfig()
				val := 16
				c.Redpanda.GroupTopicPartitions = &val
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  group_topic_partitions: 16
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with SASL enabled",
			conf: func() *Config {
				c := getValidConfig()
				enabled := true
				c.Redpanda.EnableSASL = &enabled
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  enable_sasl: true
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with pandproxy client SASL mechanism and SCRAM credentials",
			conf: func() *Config {
				c := getValidConfig()
				mechanism := "abc"
				username := "user"
				password := "pass"
				c.PandaproxyClient = &KafkaClient{
					SASLMechanism: &mechanism,
					SCRAMUsername: &username,
					SCRAMPassword: &password,
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
pandaproxy_client:
  sasl_mechanism: abc
  scram_password: pass
  scram_username: user
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write config with archival configuration",
			conf: func() *Config {
				c := getValidConfig()
				enabled := true
				access := "access"
				bucket := "bucket"
				region := "region"
				secret := "secret"
				interval := 20000
				conns := 4
				endpoint := "http"
				tls := true
				port := 100
				trustfile := "trust"
				c.Redpanda.CloudStorageEnabled = &enabled
				c.Redpanda.CloudStorageAccessKey = &access
				c.Redpanda.CloudStorageBucket = &bucket
				c.Redpanda.CloudStorageRegion = &region
				c.Redpanda.CloudStorageSecretKey = &secret
				c.Redpanda.CloudStorageReconciliationIntervalMs = &interval
				c.Redpanda.CloudStorageMaxConnections = &conns
				c.Redpanda.CloudStorageApiEndpoint = &endpoint
				c.Redpanda.CloudStorageDisableTls = &tls
				c.Redpanda.CloudStorageApiEndpointPort = &port
				c.Redpanda.CloudStorageTrustFile = &trustfile
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  cloud_storage_access_key: access
  cloud_storage_api_endpoint: http
  cloud_storage_api_endpoint_port: 100
  cloud_storage_bucket: bucket
  cloud_storage_disable_tls: true
  cloud_storage_enabled: true
  cloud_storage_max_connections: 4
  cloud_storage_reconciliation_interval_ms: 20000
  cloud_storage_region: region
  cloud_storage_secret_key: secret
  cloud_storage_trust_file: trust
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write a valid config file with two superusers",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.Superusers = []string{
					"jerry",
					"garcia",
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
  superusers:
  - jerry
  - garcia
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "shall write a valid config file with scram configured",
			conf: func() *Config {
				c := getValidConfig()
				c.Rpk.KafkaApi.SASL = &SASL{
					User:      "scram_user",
					Password:  "scram_password",
					Mechanism: "SCRAM-SHA-256",
				}
				return c
			},
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  data_directory: /var/lib/redpanda/data
  developer_mode: false
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers:
  - host:
      address: 127.0.0.1
      port: 33145
  - host:
      address: 127.0.0.1
      port: 33146
rpk:
  coredump_dir: /var/lib/redpanda/coredumps
  enable_memory_locking: true
  enable_usage_stats: true
  kafka_api:
    sasl:
      password: scram_password
      type: SCRAM-SHA-256
      user: scram_user
  overprovisioned: false
  tune_aio_events: true
  tune_clocksource: true
  tune_coredump: true
  tune_cpu: true
  tune_disk_irq: true
  tune_disk_nomerges: true
  tune_disk_scheduler: true
  tune_disk_write_cache: true
  tune_fstrim: true
  tune_network: true
  tune_swappiness: true
  tune_transparent_hugepages: true
  well_known_io: vendor:vm:storage
schema_registry: {}
`,
		},
		{
			name: "should update an existing config with single kafka_api & advertised_kafka_api obj to a list",
			existingConf: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_doc_dir: /usr/share/redpanda/admin-api-doc
  auto_create_topics_enabled: true
  data_directory: /var/lib/redpanda/data
  default_window_sec: 100
  kafka_api:
    address: 0.0.0.0
    port: 9092
  advertised_kafka_api:
    address: 1.cluster.redpanda.io
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  target_quota_byte_rate: 1000000
`,
			conf:    Default,
			wantErr: false,
			expected: `config_file: /etc/redpanda/redpanda.yaml
pandaproxy: {}
redpanda:
  admin:
  - address: 0.0.0.0
    port: 9644
  admin_api_doc_dir: /usr/share/redpanda/admin-api-doc
  advertised_kafka_api:
  - address: 1.cluster.redpanda.io
    port: 9092
  auto_create_topics_enabled: true
  data_directory: /var/lib/redpanda/data
  default_window_sec: 100
  developer_mode: true
  kafka_api:
  - address: 0.0.0.0
    port: 9092
  node_id: 0
  rpc_server:
    address: 0.0.0.0
    port: 33145
  seed_servers: []
  target_quota_byte_rate: 1000000
rpk:
  coredump_dir: /var/lib/redpanda/coredump
  enable_memory_locking: false
  enable_usage_stats: false
  overprovisioned: false
  tune_aio_events: false
  tune_clocksource: false
  tune_coredump: false
  tune_cpu: false
  tune_disk_irq: false
  tune_disk_nomerges: false
  tune_disk_scheduler: false
  tune_disk_write_cache: false
  tune_fstrim: false
  tune_network: false
  tune_swappiness: false
  tune_transparent_hugepages: false
schema_registry: {}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.conf().ConfigFile
			fs := afero.NewMemMapFs()
			mgr := NewManager(fs)
			if tt.existingConf != "" {
				_, err := utils.WriteBytes(fs, []byte(tt.existingConf), path)
				require.NoError(t, err)
			} else {
				// Write the config file so that WriteConfig backs it up
				// when the new one is written.
				err := vyaml.Persist(fs, tt.conf(), path)
				require.NoError(t, err)
			}
			_, err := mgr.Read(path)
			require.NoError(t, err)
			err = mgr.Write(tt.conf())
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			contentBytes, err := afero.ReadFile(fs, path)
			require.NoError(t, err)
			content := string(contentBytes)
			require.Equal(t, tt.expected, content)
			backup, err := findBackup(fs, filepath.Dir(path))
			require.NoError(t, err)
			_, err = fs.Stat(backup)
			require.NoError(t, err)
		})
	}
}

func TestWriteLoaded(t *testing.T) {
	fs := afero.NewMemMapFs()
	mgr := NewManager(fs)
	err := mgr.Set(
		"rpk.admin",
		`[{"address": "192.168.54.2","port": 9092}]`,
		"json",
	)
	require.NoError(t, err)

	mgr.WriteLoaded()
	conf, err := mgr.Get()
	require.NoError(t, err)

	newMgr := NewManager(fs)
	newConf, err := newMgr.Read(Default().ConfigFile)
	require.NoError(t, err)

	require.Exactly(t, conf, newConf)
}

func TestReadOrGenerate(t *testing.T) {
	tests := []struct {
		name        string
		setup       func(afero.Fs) error
		configFile  string
		check       func(st *testing.T, conf *Config)
		expectError bool
	}{
		{
			name:       "it should generate a config file at the given location",
			configFile: Default().ConfigFile,
		},
		{
			name: "it shouldn't fail if there's a config file already",
			setup: func(fs afero.Fs) error {
				conf := Default()
				mgr := NewManager(fs)
				return mgr.Write(conf)
			},
			configFile: Default().ConfigFile,
		},
		{
			name: "it should fail if the existing config file's content isn't valid yaml",
			setup: func(fs afero.Fs) error {
				bs := []byte(`redpanda:
- something`)
				return vyaml.Persist(fs, bs, Default().ConfigFile)
			},
			configFile:  Default().ConfigFile,
			expectError: true,
		},
		{
			name:       "it should set config_file to the right value",
			configFile: "./redpanda.yaml",
			check: func(st *testing.T, conf *Config) {
				path, err := filepath.Abs("./redpanda.yaml")
				require.NoError(st, err)
				require.Exactly(st, conf.ConfigFile, path)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			mgr := NewManager(fs)
			if tt.setup != nil {
				err := tt.setup(fs)
				require.NoError(t, err)
			}
			_, err := readOrGenerate(fs, InitViper(fs), tt.configFile)
			if tt.expectError {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			conf, err := mgr.Read(tt.configFile)
			require.NoError(t, err)
			if tt.check != nil {
				tt.check(t, conf)
			}
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
				TuneFstrim:         val,
				TuneCpu:            val,
				TuneAioEvents:      val,
				TuneClocksource:    val,
				TuneSwappiness:     val,
				CoredumpDir:        conf.Rpk.CoredumpDir,
				Overprovisioned:    !val,
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
				conf.Rpk.AdminApi = RpkAdminApi{
					Addresses: []string{"some.addr.com:33145"},
				}
				conf.Rpk.KafkaApi = RpkKafkaApi{
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
				conf.Rpk.AdminApi = RpkAdminApi{
					Addresses: []string{"some.addr.com:33145"},
				}
				conf.Rpk.KafkaApi = RpkKafkaApi{
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
				c.Redpanda.Id = -100
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
				c.Redpanda.KafkaApi[0].Port = 0
				return c
			},
			expected: []string{"redpanda.kafka_api.0.port can't be 0"},
		},
		{
			name: "shall return an error when the Kafka API address is empty",
			conf: func() *Config {
				c := getValidConfig()
				c.Redpanda.KafkaApi[0].Address = ""
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
			expected: []string{"if rpk.tune_coredump is set to true," +
				"rpk.coredump_dir can't be empty"},
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

func TestReadAsJSON(t *testing.T) {
	tests := []struct {
		name           string
		before         func(fs afero.Fs) error
		path           string
		expected       string
		expectedErrMsg string
	}{
		{
			name: "it should load the config as JSON",
			before: func(fs afero.Fs) error {
				conf := Default()
				conf.Redpanda.KafkaApi[0].Name = "internal"
				mgr := NewManager(fs)
				return mgr.Write(conf)
			},
			path:     Default().ConfigFile,
			expected: `{"config_file":"/etc/redpanda/redpanda.yaml","pandaproxy":{},"redpanda":{"admin":[{"address":"0.0.0.0","port":9644}],"data_directory":"/var/lib/redpanda/data","developer_mode":true,"kafka_api":[{"address":"0.0.0.0","name":"internal","port":9092}],"node_id":0,"rpc_server":{"address":"0.0.0.0","port":33145},"seed_servers":[]},"rpk":{"coredump_dir":"/var/lib/redpanda/coredump","enable_memory_locking":false,"enable_usage_stats":false,"overprovisioned":false,"tune_aio_events":false,"tune_clocksource":false,"tune_coredump":false,"tune_cpu":false,"tune_disk_irq":false,"tune_disk_nomerges":false,"tune_disk_scheduler":false,"tune_disk_write_cache":false,"tune_fstrim":false,"tune_network":false,"tune_swappiness":false,"tune_transparent_hugepages":false},"schema_registry":{}}`,
		},
		{
			name:           "it should fail if the the config isn't found",
			path:           Default().ConfigFile,
			expectedErrMsg: "open /etc/redpanda/redpanda.yaml: file does not exist",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			fs := afero.NewMemMapFs()
			mgr := NewManager(fs)
			if tt.before != nil {
				require.NoError(st, tt.before(fs))
			}
			actual, err := mgr.ReadAsJSON(tt.path)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			require.Equal(st, tt.expected, actual)
		})
	}
}

func TestReadFlat(t *testing.T) {
	expected := map[string]string{
		"config_file":                                  "/etc/redpanda/redpanda.yaml",
		"pandaproxy":                                   "",
		"redpanda.admin.0":                             "internal://192.168.92.34:9644",
		"redpanda.admin.1":                             "127.0.0.1:9645",
		"redpanda.admin_api_tls.0.cert_file":           "some/cert/file.crt",
		"redpanda.admin_api_tls.0.key_file":            "/some/key/file.pem",
		"redpanda.admin_api_tls.0.name":                "internal",
		"redpanda.admin_api_tls.1.cert_file":           "some/other/cert.crt",
		"redpanda.admin_api_tls.1.enabled":             "true",
		"redpanda.admin_api_tls.1.key_file":            "/some/other/file.pem",
		"redpanda.admin_api_tls.1.name":                "external",
		"redpanda.admin_api_tls.1.truststore_file":     "/some/other/truststore.pem",
		"redpanda.admin_api_tls.1.require_client_auth": "true",
		"redpanda.advertised_kafka_api.0":              "internal://127.0.0.1:9092",
		"redpanda.advertised_kafka_api.1":              "127.0.0.1:9093",
		"redpanda.data_directory":                      "/var/lib/redpanda/data",
		"redpanda.kafka_api.0":                         "internal://192.168.92.34:9092",
		"redpanda.kafka_api.1":                         "127.0.0.1:9093",
		"redpanda.kafka_api_tls.0.cert_file":           "some/cert/file.crt",
		"redpanda.kafka_api_tls.0.key_file":            "/some/key/file.pem",
		"redpanda.kafka_api_tls.0.name":                "internal",
		"redpanda.kafka_api_tls.1.cert_file":           "some/other/cert.crt",
		"redpanda.kafka_api_tls.1.enabled":             "true",
		"redpanda.kafka_api_tls.1.key_file":            "/some/other/file.pem",
		"redpanda.kafka_api_tls.1.name":                "external",
		"redpanda.kafka_api_tls.1.truststore_file":     "/some/other/truststore.pem",
		"redpanda.kafka_api_tls.1.require_client_auth": "true",
		"redpanda.node_id":                             "0",
		"redpanda.rpc_server":                          "0.0.0.0:33145",
		"redpanda.seed_servers.0":                      "192.168.167.0:1337",
		"redpanda.seed_servers.1":                      "192.168.167.1:1337",
		"redpanda.developer_mode":                      "true",
		"rpk.coredump_dir":                             "/var/lib/redpanda/coredump",
		"rpk.enable_memory_locking":                    "false",
		"rpk.enable_usage_stats":                       "false",
		"rpk.overprovisioned":                          "false",
		"rpk.tune_aio_events":                          "false",
		"rpk.tune_clocksource":                         "false",
		"rpk.tune_coredump":                            "false",
		"rpk.tune_cpu":                                 "false",
		"rpk.tune_disk_irq":                            "false",
		"rpk.tune_disk_nomerges":                       "false",
		"rpk.tune_disk_scheduler":                      "false",
		"rpk.tune_disk_write_cache":                    "false",
		"rpk.tune_fstrim":                              "false",
		"rpk.tune_network":                             "false",
		"rpk.tune_swappiness":                          "false",
		"rpk.tune_transparent_hugepages":               "false",
		"schema_registry":                              "",
	}
	fs := afero.NewMemMapFs()
	mgr := NewManager(fs)
	conf := Default()
	conf.Redpanda.SeedServers = []SeedServer{
		{
			SocketAddress{"192.168.167.0", 1337},
		}, {
			SocketAddress{"192.168.167.1", 1337},
		},
	}
	conf.Redpanda.AdvertisedKafkaApi = []NamedSocketAddress{{
		SocketAddress: SocketAddress{
			Address: "127.0.0.1",
			Port:    9092,
		},
		Name: "internal",
	}, {
		SocketAddress: SocketAddress{
			Address: "127.0.0.1",
			Port:    9093,
		},
	}}

	conf.Redpanda.KafkaApi = []NamedSocketAddress{{
		SocketAddress: SocketAddress{
			Address: "192.168.92.34",
			Port:    9092,
		},
		Name: "internal",
	}, {
		SocketAddress: SocketAddress{
			Address: "127.0.0.1",
			Port:    9093,
		},
	}}

	conf.Redpanda.KafkaApiTLS = []ServerTLS{{
		Name:     "internal",
		KeyFile:  "/some/key/file.pem",
		CertFile: "some/cert/file.crt",
	}, {
		Name:              "external",
		KeyFile:           "/some/other/file.pem",
		CertFile:          "some/other/cert.crt",
		TruststoreFile:    "/some/other/truststore.pem",
		Enabled:           true,
		RequireClientAuth: true,
	}}

	conf.Redpanda.AdminApi = []NamedSocketAddress{{
		SocketAddress: SocketAddress{
			Address: "192.168.92.34",
			Port:    9644,
		},
		Name: "internal",
	}, {
		SocketAddress: SocketAddress{
			Address: "127.0.0.1",
			Port:    9645,
		},
	}}

	conf.Redpanda.AdminApiTLS = []ServerTLS{{
		Name:     "internal",
		KeyFile:  "/some/key/file.pem",
		CertFile: "some/cert/file.crt",
	}, {
		Name:              "external",
		KeyFile:           "/some/other/file.pem",
		CertFile:          "some/other/cert.crt",
		TruststoreFile:    "/some/other/truststore.pem",
		Enabled:           true,
		RequireClientAuth: true,
	}}

	err := mgr.Write(conf)
	require.NoError(t, err)
	props, err := mgr.ReadFlat(conf.ConfigFile)
	require.NoError(t, err)
	require.NotEqual(t, 0, len(props))

	require.Exactly(t, expected, props)
}

func TestWriteAndGenerateNodeUuid(t *testing.T) {
	fs := afero.NewMemMapFs()
	mgr := NewManager(fs)
	baseDir := "/etc/redpanda"
	path := baseDir + "/redpanda.yaml"
	conf := getValidConfig()
	conf.ConfigFile = path
	bs, err := yaml.Marshal(conf)
	require.NoError(t, err)
	err = fs.MkdirAll(baseDir, 0755)
	require.NoError(t, err)
	_, err = utils.WriteBytes(fs, bs, path)
	require.NoError(t, err)
	err = mgr.WriteNodeUUID(conf)
	require.NoError(t, err)
	require.NotEqual(t, "", conf.NodeUuid)
	readConf, err := mgr.Read(path)
	require.NoError(t, err)
	require.Exactly(t, conf, readConf)
}

func TestGet(t *testing.T) {
	mgr := NewManager(afero.NewMemMapFs())
	conf, err := mgr.Get()
	require.NoError(t, err)
	require.Exactly(t, Default(), conf)
}
