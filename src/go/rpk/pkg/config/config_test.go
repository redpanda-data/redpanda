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
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/utils"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func testRedpandaYaml() *RedpandaYaml {
	y := DevDefault()
	y.Redpanda.SeedServers = []SeedServer{
		{Host: SocketAddress{"127.0.0.1", 33145}},
		{Host: SocketAddress{"127.0.0.1", 33146}},
	}
	y.Redpanda.DeveloperMode = false
	y.Rpk = RpkNodeConfig{
		EnableMemoryLocking: true,
		Tuners: RpkNodeTuners{
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
			TuneCoredump:             true,
			TuneBallastFile:          true,
			CoredumpDir:              "/var/lib/redpanda/coredumps",
			WellKnownIo:              "vendor:vm:storage",
		},
	}
	return y
}

func TestSet(t *testing.T) {
	authNSasl := "sasl"
	tests := []struct {
		name      string
		key       string
		value     string
		check     func(st *testing.T, y *RedpandaYaml)
		expectErr bool
	}{
		{
			name:  "parse '1' as an int and not as bool (true)",
			key:   "redpanda.node_id",
			value: "1",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 1, *y.Redpanda.ID)
			},
		},
		{
			name:  "set single integer fields",
			key:   "redpanda.node_id",
			value: "54312",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 54312, *y.Redpanda.ID)
			},
		},
		{
			name:  "detect single integer fields if format isn't passed",
			key:   "redpanda.node_id",
			value: "54312",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 54312, *y.Redpanda.ID)
			},
		},
		{
			name:  "set single float fields",
			key:   "redpanda.float_field",
			value: "42.3",
			check: func(st *testing.T, cfg *RedpandaYaml) {
				require.Exactly(st, 42.3, cfg.Redpanda.Other["float_field"])
			},
		},
		{
			name:  "detect single float fields if format isn't passed",
			key:   "redpanda.float_field",
			value: "42.3",
			check: func(st *testing.T, cfg *RedpandaYaml) {
				require.Exactly(st, 42.3, cfg.Redpanda.Other["float_field"])
			},
		},
		{
			name:  "set single string fields",
			key:   "redpanda.data_directory",
			value: "/var/lib/differentdir",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "/var/lib/differentdir", y.Redpanda.Directory)
			},
		},
		{
			name:  "detect single string fields if format isn't passed",
			key:   "redpanda.data_directory",
			value: "/var/lib/differentdir",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "/var/lib/differentdir", y.Redpanda.Directory)
			},
		},
		{
			name:  "set single bool fields",
			key:   "rpk.tune_network",
			value: "true",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, true, y.Rpk.Tuners.TuneNetwork)
			},
		},
		{
			name:  "set single bool fields in Other fields (json)",
			key:   "redpanda.enable_metrics_test",
			value: "true",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, true, y.Redpanda.Other["enable_metrics_test"])
			},
		},
		{
			name:  "set single number in Other fields (json)",
			key:   "redpanda.timeout_test",
			value: "123991",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 123991, y.Redpanda.Other["timeout_test"])
			},
		},
		{
			name:  "set single strings in Other fields (json)",
			key:   "redpanda.test_name",
			value: `"my_name"`,
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "my_name", y.Redpanda.Other["test_name"])
			},
		},
		{
			name:  "set objects in Other fields (json)",
			key:   "redpanda.my_object",
			value: `{"name":"test","enabled":true}`,
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, map[string]interface{}{
					"name":    "test",
					"enabled": true,
				}, y.Redpanda.Other["my_object"])
			},
		},
		{
			name:  "detect single bool fields if format isn't passed",
			key:   "rpk.tune_cpu",
			value: "true",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, true, y.Rpk.Tuners.TuneCPU)
			},
		},
		{
			name: "partially set map fields (yaml)",
			key:  "rpk",
			value: `tune_disk_irq: true
tune_cpu: true`,
			check: func(st *testing.T, y *RedpandaYaml) {
				expected := RpkNodeConfig{
					Overprovisioned:     false,
					EnableMemoryLocking: false,
					Tuners: RpkNodeTuners{
						TuneNetwork:              false,
						TuneDiskScheduler:        false,
						TuneNomerges:             false,
						TuneDiskIrq:              true,
						TuneCPU:                  true,
						TuneAioEvents:            false,
						TuneClocksource:          false,
						TuneSwappiness:           false,
						TuneTransparentHugePages: false,
						TuneFstrim:               false,
						TuneCoredump:             false,
						TuneDiskWriteCache:       false,
					},
				}
				require.Exactly(st, expected, y.Rpk)
			},
		},
		{
			name:  "detect pandaproxy client single field if format isn't passed",
			key:   "pandaproxy_client.retries",
			value: "42",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 42, y.PandaproxyClient.Other["retries"])
			},
		},
		{
			name: "detect yaml-formatted values if format isn't passed",
			key:  "redpanda.kafka_api",
			value: `- name: external
  address: 192.168.73.45
  port: 9092
- name: internal
  address: 10.21.34.58
  port: 9092
`,
			check: func(st *testing.T, y *RedpandaYaml) {
				expected := []NamedAuthNSocketAddress{{
					Name:    "external",
					Address: "192.168.73.45",
					Port:    9092,
				}, {
					Name:    "internal",
					Address: "10.21.34.58",
					Port:    9092,
				}}
				require.Exactly(st, expected, y.Redpanda.KafkaAPI)
			},
		},
		{
			name: "extract kafka_api[].authentication_method",
			key:  "redpanda.kafka_api",
			value: `- name: external
  address: 192.168.73.45
  port: 9092
  authentication_method: sasl
- name: internal
  address: 10.21.34.58
  port: 9092
`,
			check: func(st *testing.T, y *RedpandaYaml) {
				expected := []NamedAuthNSocketAddress{{
					Name:    "external",
					Address: "192.168.73.45",
					Port:    9092,
					AuthN:   &authNSasl,
				}, {
					Name:    "internal",
					Address: "10.21.34.58",
					Port:    9092,
				}}
				require.Exactly(st, expected, y.Redpanda.KafkaAPI)
			},
		},
		{
			name: "partially set map fields (json)",
			key:  "redpanda.kafka_api",
			value: `[{
		  "address": "192.168.54.2",
		  "port": 9092
		}]`,
			check: func(st *testing.T, y *RedpandaYaml) {
				expected := []NamedAuthNSocketAddress{{
					Port:    9092,
					Address: "192.168.54.2",
				}}
				require.Exactly(st, expected, y.Redpanda.KafkaAPI)
			},
		},
		{
			name: "detect json-formatted values if format isn't passed",
			key:  "redpanda.advertised_kafka_api",
			value: `[{
		  "address": "192.168.54.2",
		  "port": 9092
		}]`,
			check: func(st *testing.T, y *RedpandaYaml) {
				expected := []NamedSocketAddress{{
					Port:    9092,
					Address: "192.168.54.2",
				}}
				require.Exactly(st, expected, y.Redpanda.AdvertisedKafkaAPI)
			},
		},

		{
			name:  "set value of a slice",
			key:   "redpanda.admin.port",
			value: "9641",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 9641, y.Redpanda.AdminAPI[0].Port)
			},
		},

		{
			name:  "set value of a slice with an index",
			key:   "redpanda.admin[0].port",
			value: "9641",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 9641, y.Redpanda.AdminAPI[0].Port)
			},
		},
		{
			name:  "set value of a slice with an index at end extends slice",
			key:   "redpanda.admin[1].port",
			value: "9648",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, 9648, y.Redpanda.AdminAPI[1].Port)
			},
		},

		{
			name:  "set slice single values",
			key:   "redpanda.seed_servers.host.address",
			value: "foo",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "foo", y.Redpanda.SeedServers[0].Host.Address)
			},
		},

		{
			name:  "set slice object",
			key:   "redpanda.seed_servers.host",
			value: `{address: 0.0.0.0, port: 80}`,
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "0.0.0.0", y.Redpanda.SeedServers[0].Host.Address)
				require.Exactly(st, 80, y.Redpanda.SeedServers[0].Host.Port)
			},
		},

		{
			name:  "set slice with object defaults to index 0",
			key:   "redpanda.advertised_kafka_api",
			value: `{address: 3.250.158.1, port: 9092}`,
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "3.250.158.1", y.Redpanda.AdvertisedKafkaAPI[0].Address)
				require.Exactly(st, 9092, y.Redpanda.AdvertisedKafkaAPI[0].Port)
			},
		},

		{
			name:  "set tls.enabled=true initializes empty tls",
			key:   "rpk.kafka_api.tls.enabled",
			value: "true",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, new(TLS), y.Rpk.KafkaAPI.TLS)
			},
		},
		{
			name:  "set tls.enabled=false leaves tls null",
			key:   "rpk.kafka_api.tls.enabled",
			value: "false",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, (*TLS)(nil), y.Rpk.KafkaAPI.TLS)
			},
		},
		{
			name:  "set tls={} initializes empty tls",
			key:   "rpk.kafka_api.tls",
			value: "{}",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, new(TLS), y.Rpk.KafkaAPI.TLS)
			},
		},
		{
			name:  "set kafka tls to a value",
			key:   "rpk.kafka_api.tls",
			value: "{cert_file: /etc/redpanda/certs/cert.pem, key_file: /etc/redpanda/certs/node.key, truststore_file: /etc/redpanda/certs/ca.crt}",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, &TLS{
					KeyFile:        "/etc/redpanda/certs/node.key",
					CertFile:       "/etc/redpanda/certs/cert.pem",
					TruststoreFile: "/etc/redpanda/certs/ca.crt",
				}, y.Rpk.KafkaAPI.TLS)
			},
		},
		{
			name:  "set admin tls to a value",
			key:   "rpk.admin_api.tls",
			value: "{cert_file: /etc/redpanda/certs/cert.pem, key_file: /etc/redpanda/certs/node.key, truststore_file: /etc/redpanda/certs/ca.crt}",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, &TLS{
					KeyFile:        "/etc/redpanda/certs/node.key",
					CertFile:       "/etc/redpanda/certs/cert.pem",
					TruststoreFile: "/etc/redpanda/certs/ca.crt",
				}, y.Rpk.AdminAPI.TLS)
			},
		},
		{
			name:  "set tls=null leaves tls null",
			key:   "rpk.kafka_api.tls",
			value: "null",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, (*TLS)(nil), y.Rpk.KafkaAPI.TLS)
			},
		},
		{
			name:  "set array without brackets works",
			key:   "rpk.kafka_api.brokers",
			value: "127.0.0.1,127.0.0.2",
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, (*TLS)(nil), y.Rpk.KafkaAPI.TLS)
				require.Exactly(st, []string{"127.0.0.1", "127.0.0.2"}, y.Rpk.KafkaAPI.Brokers)
			},
		},

		{
			name:  "slices with one element works",
			key:   "rpk.kafka_api.brokers",
			value: `127.0.0.0:9092`,
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "127.0.0.0:9092", y.Rpk.KafkaAPI.Brokers[0])
			},
		},
		{
			name:  "slices with one element works with indexing",
			key:   "rpk.kafka_api.brokers[0]",
			value: `127.0.0.0:9092`,
			check: func(st *testing.T, y *RedpandaYaml) {
				require.Exactly(st, "127.0.0.0:9092", y.Rpk.KafkaAPI.Brokers[0])
			},
		},

		{
			name:      "fail if the value isn't well formatted (json)",
			key:       "redpanda",
			value:     `{"seed_servers": []`,
			expectErr: true,
		},
		{
			name: "fail if the value isn't well formatted (yaml)",
			key:  "redpanda",
			value: `seed_servers:
		- host:
		  address: "123.`,
			expectErr: true,
		},
		{
			name:      "fail if no key is passed",
			value:     `node_id=1`,
			expectErr: true,
		},
		{
			name:      "fail if deep unrecognized value is passed",
			key:       "redpanda.unrecognized.name",
			value:     "foo",
			expectErr: true,
		},

		{
			name:      "invalid negative index",
			key:       "redpanda.admin[-1].port",
			value:     "9641",
			expectErr: true,
		},
		{
			name:      "invalid large index",
			key:       "redpanda.admin[12310293812093823094801].port",
			value:     "9641",
			expectErr: true,
		},
		{
			name:      "invalid out of bounds index",
			key:       "redpanda.admin[2].port", // 0 is default, 1 is valid (extends by one), 2 is invalid
			value:     "9641",
			expectErr: true,
		},
		{
			name:      "index into other (unknown) field",
			key:       "redpanda.fiz[0]",
			value:     "9641",
			expectErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()
			cfg, err := new(Params).Load(fs)
			require.NoError(t, err)
			y := cfg.VirtualRedpandaYaml()
			err = Set(y, tt.key, tt.value)
			if tt.expectErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.check(t, y)
		})
	}
}

func TestDevDefault(t *testing.T) {
	defaultConfig := DevDefault()
	expected := &RedpandaYaml{
		fileLocation:   DefaultRedpandaYamlPath,
		Pandaproxy:     &Pandaproxy{},
		SchemaRegistry: &SchemaRegistry{},
		Redpanda: RedpandaNodeConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{"0.0.0.0", 33145},
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
			Overprovisioned: true,
			Tuners: RpkNodeTuners{
				CoredumpDir: "/var/lib/redpanda/coredump",
			},
		},
	}
	require.Exactly(t, expected, defaultConfig)
}

func TestProdDefault(t *testing.T) {
	defaultConfig := ProdDefault()
	expected := &RedpandaYaml{
		fileLocation:   DefaultRedpandaYamlPath,
		Pandaproxy:     &Pandaproxy{},
		SchemaRegistry: &SchemaRegistry{},
		Redpanda: RedpandaNodeConfig{
			Directory: "/var/lib/redpanda/data",
			RPCServer: SocketAddress{"0.0.0.0", 33145},
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
			DeveloperMode: false,
		},
		Rpk: RpkNodeConfig{
			Overprovisioned: false,
			Tuners: RpkNodeTuners{
				CoredumpDir:        "/var/lib/redpanda/coredump",
				TuneAioEvents:      true,
				TuneBallastFile:    true,
				TuneCPU:            true,
				TuneClocksource:    true,
				TuneDiskIrq:        true,
				TuneDiskScheduler:  true,
				TuneDiskWriteCache: true,
				TuneFstrim:         false,
				TuneNetwork:        true,
				TuneNomerges:       true,
				TuneSwappiness:     true,
			},
		},
	}
	require.Exactly(t, expected, defaultConfig)
}

// This somewhat duplicates TestParamsRedpandaYamlWrite but has some different
// configurations. TODO: merge tests.
func TestWrite(t *testing.T) {
	tests := []struct {
		name         string
		existingConf string
		conf         func() *RedpandaYaml
		wantErr      bool
		expected     string
	}{
		{
			name: "write default values",
			conf: testRedpandaYaml,
			expected: `redpanda:
    data_directory: /var/lib/redpanda/data
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
rpk:
    enable_memory_locking: true
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
    tune_coredump: true
    coredump_dir: /var/lib/redpanda/coredumps
    tune_ballast_file: true
    well_known_io: vendor:vm:storage
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "write additional values",
			conf: func() *RedpandaYaml {
				y := testRedpandaYaml()
				y.Redpanda.AdvertisedRPCAPI = &SocketAddress{
					"174.32.64.2",
					33145,
				}
				return y
			},
			expected: `redpanda:
    data_directory: /var/lib/redpanda/data
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
rpk:
    enable_memory_locking: true
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
    tune_coredump: true
    coredump_dir: /var/lib/redpanda/coredumps
    tune_ballast_file: true
    well_known_io: vendor:vm:storage
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "write if empty struct is passed",
			conf: func() *RedpandaYaml {
				y := testRedpandaYaml()
				y.Rpk = RpkNodeConfig{}
				return y
			},
			wantErr: false,
			expected: `redpanda:
    data_directory: /var/lib/redpanda/data
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
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "write unrecognized values ('Other' map).",
			conf: func() *RedpandaYaml {
				y := testRedpandaYaml()
				size := 536870912
				if y.Redpanda.Other == nil {
					y.Redpanda.Other = make(map[string]interface{})
				}
				y.Redpanda.Other["log_segment_size"] = &size
				return y
			},
			wantErr: false,
			expected: `redpanda:
    data_directory: /var/lib/redpanda/data
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
    log_segment_size: 536870912
rpk:
    enable_memory_locking: true
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
    tune_coredump: true
    coredump_dir: /var/lib/redpanda/coredumps
    tune_ballast_file: true
    well_known_io: vendor:vm:storage
pandaproxy: {}
schema_registry: {}
`,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			path := tt.conf().FileLocation()
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
			require.Equal(t, tt.expected, content)
		})
	}
}

func TestSetMode(t *testing.T) {
	fillRpkNodeConfig := func(mode string) func() *RedpandaYaml {
		return func() *RedpandaYaml {
			conf := DevDefault()
			val := mode == ModeProd
			conf.Redpanda.DeveloperMode = !val
			conf.Rpk = RpkNodeConfig{
				Overprovisioned: !val,
				Tuners: RpkNodeTuners{
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
					CoredumpDir:        conf.Rpk.Tuners.CoredumpDir,
					TuneBallastFile:    val,
				},
			}
			return conf
		}
	}

	tests := []struct {
		name           string
		mode           string
		startingConf   func() *RedpandaYaml
		expectedConfig func() *RedpandaYaml
		expectedErrMsg string
	}{
		{
			name:           "it should disable all tuners for dev mode",
			mode:           ModeDev,
			expectedConfig: fillRpkNodeConfig(ModeDev),
		},
		{
			name:           "it should disable all tuners for dev mode ('development')",
			mode:           "development",
			expectedConfig: fillRpkNodeConfig(ModeDev),
		},
		{
			name:           "it should disable all tuners for dev mode ('')",
			mode:           "",
			expectedConfig: fillRpkNodeConfig(ModeDev),
		},
		{
			name:           "it should enable all the default tuners for prod mode",
			mode:           ModeProd,
			expectedConfig: fillRpkNodeConfig(ModeProd),
		},
		{
			name:           "it should enable all the default tuners for prod mode ('production')",
			mode:           ModeProd,
			expectedConfig: fillRpkNodeConfig(ModeProd),
		},
		{
			name:           "it should return an error for invalid modes",
			mode:           "winning",
			expectedErrMsg: "unknown mode \"winning\"",
		},
		{
			name: "it should preserve all the values that shouldn't be reset",
			startingConf: func() *RedpandaYaml {
				conf := DevDefault()
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
			expectedConfig: func() *RedpandaYaml {
				conf := fillRpkNodeConfig(ModeProd)()
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
			y := DevDefault()
			if tt.startingConf != nil {
				y = tt.startingConf()
			}
			c := Config{
				redpandaYamlActual: *y,
				redpandaYamlExists: true,
			}
			fs := afero.NewMemMapFs()
			err := c.SetMode(fs, tt.mode)
			if tt.expectedErrMsg != "" {
				require.EqualError(t, err, tt.expectedErrMsg)
				return
			}
			require.NoError(t, err)
			require.Exactly(t, tt.expectedConfig(), &c.redpandaYamlActual)
		})
	}
}

func TestCheckConfig(t *testing.T) {
	tests := []struct {
		name     string
		conf     func() *RedpandaYaml
		expected []string
	}{
		{
			name:     "shall return no errors when config is valid",
			conf:     testRedpandaYaml,
			expected: []string{},
		},
		{
			name: "shall return an error when config file does not contain data directory setting",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.Directory = ""
				return c
			},
			expected: []string{"redpanda.data_directory can't be empty"},
		},
		{
			name: "shall return an error when id of server is negative",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.ID = new(int)
				*c.Redpanda.ID = -100
				return c
			},
			expected: []string{"redpanda.node_id can't be a negative integer"},
		},
		{
			name: "shall return an error when the RPC server port is 0",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.RPCServer.Port = 0
				return c
			},
			expected: []string{"redpanda.rpc_server.port can't be 0"},
		},
		{
			name: "shall return an error when the RPC server address is empty",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.RPCServer.Address = ""
				return c
			},
			expected: []string{"redpanda.rpc_server.address can't be empty"},
		},
		{
			name: "shall return an error when the Kafka API port is 0",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.KafkaAPI[0].Port = 0
				return c
			},
			expected: []string{"redpanda.kafka_api[0].port can't be 0"},
		},
		{
			name: "shall return an error when the Kafka API address is empty",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.KafkaAPI[0].Address = ""
				return c
			},
			expected: []string{"redpanda.kafka_api[0].address can't be empty"},
		},
		{
			name: "shall return an error when one of the seed servers' address is empty",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.SeedServers[0].Host.Address = ""
				return c
			},
			expected: []string{"redpanda.seed_servers[0].host.address can't be empty"},
		},
		{
			name: "shall return an error when one of the seed servers' port is 0",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Redpanda.SeedServers[1].Host.Port = 0
				return c
			},
			expected: []string{"redpanda.seed_servers[1].host.port can't be 0"},
		},
		{
			name: "shall return no errors when tune_coredump is set to false," +
				"regardless of coredump_dir's value",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Rpk.Tuners.TuneCoredump = false
				c.Rpk.Tuners.CoredumpDir = ""
				return c
			},
			expected: []string{},
		},
		{
			name: "shall return an error when tune_coredump is set to true," +
				"but coredump_dir is empty",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Rpk.Tuners.CoredumpDir = ""
				return c
			},
			expected: []string{"if rpk.tune_coredump is set to true, rpk.coredump_dir can't be empty"},
		},
		{
			name: "shall return no error if setup is empty," +
				"but coredump_dir is empty",
			conf: func() *RedpandaYaml {
				c := testRedpandaYaml()
				c.Rpk.Tuners.WellKnownIo = ""
				return c
			},
			expected: []string{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, got := tt.conf().Check()
			errMsgs := []string{}
			for _, err := range got {
				errMsgs = append(errMsgs, err.Error())
			}
			require.Exactly(t, tt.expected, errMsgs)
		})
	}
}
