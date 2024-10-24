// Copyright 2021 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

//go:build linux
// +build linux

package redpanda

import (
	"strings"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestBootstrap(t *testing.T) {
	for _, test := range []struct {
		name     string
		ips      []string
		self     string
		id       string
		advKafka string
		advRPC   string
		cfgFile  string

		exp string
	}{
		{
			name: "sets defaults with node_id",
			id:   "1",
			self: "192.168.34.5",

			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    node_id: 1
    seed_servers: []
    rpc_server:
        address: 192.168.34.5
        port: 33145
    kafka_api:
        - address: 192.168.34.5
          port: 9092
    admin:
        - address: 192.168.34.5
          port: 9644
    advertised_rpc_api:
        address: 192.168.34.5
        port: 33145
    advertised_kafka_api:
        - address: 192.168.34.5
          port: 9092
    developer_mode: true
rpk:
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
		},

		{
			name: "fill seed servers with ip and hostnames",
			ips:  []string{"187.89.76.3", "192.168.34.5", "localhost", "192.168.45.8", "redpanda.com"},
			self: "192.168.34.5",
			id:   "1",

			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    node_id: 1
    seed_servers:
        - host:
            address: 187.89.76.3
            port: 33145
        - host:
            address: 192.168.34.5
            port: 33145
        - host:
            address: localhost
            port: 33145
        - host:
            address: 192.168.45.8
            port: 33145
        - host:
            address: redpanda.com
            port: 33145
    rpc_server:
        address: 192.168.34.5
        port: 33145
    kafka_api:
        - address: 192.168.34.5
          port: 9092
    admin:
        - address: 192.168.34.5
          port: 9644
    advertised_rpc_api:
        address: 192.168.34.5
        port: 33145
    advertised_kafka_api:
        - address: 192.168.34.5
          port: 9092
    developer_mode: true
rpk:
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name:     "parse advertised addresses and ports",
			id:       "1",
			self:     "192.168.34.5",
			advRPC:   "200.200.200.1:8812",
			advKafka: "127.12.1.1:9923",
			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    node_id: 1
    seed_servers: []
    rpc_server:
        address: 192.168.34.5
        port: 33145
    kafka_api:
        - address: 192.168.34.5
          port: 9092
    admin:
        - address: 192.168.34.5
          port: 9644
    advertised_rpc_api:
        address: 200.200.200.1
        port: 8812
    advertised_kafka_api:
        - address: 127.12.1.1
          port: 9923
    developer_mode: true
rpk:
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
		},
		{
			name: "modify existing file default and preserve modifications",
			ips:  []string{"127.0.0.1"},
			self: "192.168.34.5",
			id:   "2",

			// rpc_server address modified, port is default
			// kafka_api address is default, port modified
			// admin address and port are modified
			//
			// node_id modified but reverted back to flag value of 2
			cfgFile: `redpanda:
    data_directory: /foo
    node_id: 5
    seed_servers:
        - host:
            address: 127.0.0.1
            port: 33145
    rpc_server:
        address: 127.0.0.5
        port: 33145
    kafka_api:
        - address: 0.0.0.0
          port: 9093
    admin:
        - address: 127.0.03
          port: 5677
`,
			exp: `redpanda:
    data_directory: /foo
    node_id: 2
    seed_servers:
        - host:
            address: 127.0.0.1
            port: 33145
    rpc_server:
        address: 127.0.0.5
        port: 33145
    kafka_api:
        - address: 192.168.34.5
          port: 9093
    admin:
        - address: 127.0.03
          port: 5677
    advertised_rpc_api:
        address: 192.168.34.5
        port: 33145
    advertised_kafka_api:
        - address: 192.168.34.5
          port: 9092
`,
		},
		{
			name: "existing file with modifications 2",
			ips:  []string{"127.0.0.1"},
			self: "192.168.34.5",
			id:   "3",

			// rpc_server custom port, default host
			// kafka_api modified, 0.0.0.0 kept because multiple of elements
			// admin api missing, added as default
			// both advertised modified.
			cfgFile: `redpanda:
    data_directory: /foo
    node_id: 5
    seed_servers:
        - host:
            address: 127.0.0.1
            port: 33145
    rpc_server:
        address: 0.0.0.0
        port: 33333
    kafka_api:
        - address: 0.0.0.0
          port: 9093
        - address: 127.0.03
          port: 5677
    advertised_rpc_api:
        address: 100.0.1.234
        port: 33221
    advertised_kafka_api:
        - address: 127.0.0.1
          port: 9088
`,
			exp: `redpanda:
    data_directory: /foo
    node_id: 3
    seed_servers:
        - host:
            address: 127.0.0.1
            port: 33145
    rpc_server:
        address: 192.168.34.5
        port: 33333
    kafka_api:
        - address: 0.0.0.0
          port: 9093
        - address: 127.0.03
          port: 5677
    admin:
        - address: 192.168.34.5
          port: 9644
    advertised_rpc_api:
        address: 100.0.1.234
        port: 33221
    advertised_kafka_api:
        - address: 127.0.0.1
          port: 9088
`,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()

			if test.cfgFile != "" {
				err := afero.WriteFile(fs, config.DefaultRedpandaYamlPath, []byte(test.cfgFile), 0o644)
				if err != nil {
					t.Errorf("unexpected failure writing passed config file: %v", err)
					return
				}
			}

			c := bootstrap(fs, new(config.Params))

			var args []string
			if len(test.ips) != 0 {
				args = append(args, "--ips", strings.Join(test.ips, ","))
			}
			if test.self != "" {
				args = append(args, "--self", test.self)
			}
			if test.id != "" {
				args = append(args, "--id", test.id)
			}
			if test.advKafka != "" {
				args = append(args, "--advertised-kafka", test.advKafka)
			}
			if test.advRPC != "" {
				args = append(args, "--advertised-rpc", test.advRPC)
			}
			c.SetArgs(args)

			if err := c.Execute(); err != nil {
				t.Errorf("unexpected err: %v", err)
				return
			}

			file, err := afero.ReadFile(fs, config.DefaultRedpandaYamlPath)
			if err != nil {
				t.Errorf("unexpected failure reading config file: %v", err)
				return
			}
			if got := string(file); got != test.exp {
				t.Errorf("got file != exp file:\ngot:\n%s\nexp:\n%s\n\n", got, test.exp)
			}
		})
	}
}

// This is a top level command test, individual cases for set are
// tested in 'rpk/pkg/config/config_test.go'.
func TestSetCommand(t *testing.T) {
	for _, test := range []struct {
		name    string
		cfgFile string
		args    []string
		exp     string
	}{
		{
			name: "set without config file on disk",
			exp: `redpanda:
    data_directory: /var/lib/redpanda/data
    rack: redpanda-rack
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
    overprovisioned: true
    coredump_dir: /var/lib/redpanda/coredump
pandaproxy: {}
schema_registry: {}
`,
			args: []string{"redpanda.rack", "redpanda-rack"},
		}, {
			name: "set with loaded config",
			cfgFile: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
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
    tune_network: true
    tune_disk_scheduler: true
`,
			exp: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
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
    tune_network: true
    tune_disk_scheduler: true
    tune_cpu: true
`,
			args: []string{"rpk.tune_cpu", "true"},
		}, {
			name: "set with =",
			args: []string{"rpk.tune_cpu=true"},
			cfgFile: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
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
    tune_network: true
    tune_disk_scheduler: true
`,
			exp: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
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
    tune_network: true
    tune_disk_scheduler: true
    tune_cpu: true
`,
		}, {
			name: "set with = negative number",
			args: []string{"redpanda.rpc_server.port=-1"},
			cfgFile: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
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
    tune_network: true
    tune_disk_scheduler: true
`,
			exp: `redpanda:
    data_directory: data/dir
    rack: redpanda-rack
    seed_servers: []
    rpc_server:
        address: 0.0.0.0
        port: -1
    kafka_api:
        - address: 0.0.0.0
          port: 9092
    admin:
        - address: 0.0.0.0
          port: 9644
    developer_mode: true
rpk:
    tune_network: true
    tune_disk_scheduler: true
`,
		},
	} {
		fs := afero.NewMemMapFs()

		// We create a config file in default redpanda location
		if test.cfgFile != "" {
			err := afero.WriteFile(fs, "/etc/redpanda/redpanda.yaml", []byte(test.cfgFile), 0o644)
			require.NoError(t, err, "unexpected failure writing passed config file: %v", err)
		}

		c := set(fs, new(config.Params))
		c.SetArgs(test.args)
		err := c.Execute()
		require.NoError(t, err, "error during command execution: %v", err)

		// Read back from that default location and compare.
		file, err := afero.ReadFile(fs, "/etc/redpanda/redpanda.yaml")
		require.NoError(t, err, "unexpected failure reading config file: %v", err)

		require.Equal(t, test.exp, string(file))
	}
}
