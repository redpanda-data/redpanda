// Copyright 2021 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

// +build linux

package redpanda

import (
	"bytes"
	"os"
	"testing"

	"github.com/sirupsen/logrus"
	"github.com/spf13/afero"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/config"
	"github.com/vectorizedio/redpanda/src/go/rpk/pkg/redpanda"
	rp "github.com/vectorizedio/redpanda/src/go/rpk/pkg/redpanda"
)

type noopLauncher struct {
	rpArgs *rp.RedpandaArgs
}

func (l *noopLauncher) Start(_ string, rpArgs *rp.RedpandaArgs) error {
	l.rpArgs = rpArgs
	return nil
}

func TestMergeFlags(t *testing.T) {
	tests := []struct {
		name      string
		current   map[string]interface{}
		overrides []string
		expected  map[string]string
	}{
		{
			name:      "it should override the existent values",
			current:   map[string]interface{}{"a": "true", "b": "2", "c": "127.0.0.1"},
			overrides: []string{"--a false", "b 42"},
			expected:  map[string]string{"a": "false", "b": "42", "c": "127.0.0.1"},
		}, {
			name:    "it should override the existent values (2)",
			current: map[string]interface{}{"lock-memory": "true", "cpumask": "0-1", "logger-log-level": "'exception=debug'"},
			overrides: []string{
				"--overprovisioned", "--unsafe-bypass-fsync 1",
				"--default-log-level=trace", "--logger-log-level='exception=debug'",
				"--fail-on-abandoned-failed-futures",
			},
			expected: map[string]string{
				"lock-memory":                        "true",
				"cpumask":                            "0-1",
				"logger-log-level":                   "'exception=debug'",
				"overprovisioned":                    "",
				"unsafe-bypass-fsync":                "1",
				"default-log-level":                  "trace",
				"--fail-on-abandoned-failed-futures": "",
			},
		}, {
			name:      "it should create values not present in the current flags",
			current:   map[string]interface{}{},
			overrides: []string{"b 42", "c 127.0.0.1"},
			expected:  map[string]string{"b": "42", "c": "127.0.0.1"},
		}, {
			name:      "it shouldn't change the current flags if no overrides are given",
			current:   map[string]interface{}{"b": "42", "c": "127.0.0.1"},
			overrides: []string{},
			expected:  map[string]string{"b": "42", "c": "127.0.0.1"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			flags := mergeFlags(tt.current, tt.overrides)
			require.Equal(t, len(flags), len(tt.expected))
			if len(flags) != len(tt.expected) {
				t.Fatal("the flags dicts differ in size")
			}

			for k, v := range flags {
				require.Equal(t, tt.expected[k], v)
			}
		})
	}
}

func TestParseSeeds(t *testing.T) {
	tests := []struct {
		name           string
		arg            []string
		expected       []config.SeedServer
		expectedErrMsg string
	}{
		{
			name: "it should parse well-formed seed addrs",
			arg:  []string{"127.0.0.1:1234", "domain.com:9892", "lonely-host", "192.168.34.1"},
			expected: []config.SeedServer{
				{
					config.SocketAddress{"127.0.0.1", 1234},
				},
				{
					config.SocketAddress{"domain.com", 9892},
				},
				{
					config.SocketAddress{"lonely-host", 33145},
				},
				{
					config.SocketAddress{"192.168.34.1", 33145},
				},
			},
		},
		{
			name:     "it shouldn't do anything for an empty list",
			arg:      []string{},
			expected: []config.SeedServer{},
		},

		{
			name:           "it should fail for empty addresses",
			arg:            []string{""},
			expectedErrMsg: "Couldn't parse seed '': empty address",
		},
		{
			name:           "it should fail if the host is empty",
			arg:            []string{" :1234"},
			expectedErrMsg: "Couldn't parse seed ' :1234': invalid host \" :1234\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			addrs, err := parseSeeds(tt.arg)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, addrs)
		})
	}
}

func TestStartCommand(t *testing.T) {
	tests := []struct {
		name           string
		launcher       redpanda.Launcher
		args           []string
		before         func(afero.Fs) error
		after          func()
		postCheck      func(afero.Fs, *rp.RedpandaArgs, *testing.T)
		expectedErrMsg string
	}{{
		name: "should fail if the config at the given path is corrupt",
		args: []string{"--config", config.Default().ConfigFile},
		before: func(fs afero.Fs) error {
			return afero.WriteFile(
				fs,
				config.Default().ConfigFile,
				[]byte("^&notyaml"),
				0755,
			)
		},
		expectedErrMsg: "An error happened while trying to read /etc/redpanda/redpanda.yaml: While parsing config: yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `^&notyaml` into map[string]interface {}",
	}, {
		name: "should generate the config at the given path if it doesn't exist",
		args: []string{
			"--config", config.Default().ConfigFile,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			exists, err := afero.Exists(
				fs,
				path,
			)
			require.NoError(st, err)
			require.True(
				st,
				exists,
				"The config should have been created at '%s'",
				path,
			)
			defaultConf := config.Default()
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			require.Exactly(st, defaultConf, conf)
		},
	}, {
		name: "it should write the given config file path",
		args: []string{
			"--config", "/arbitrary/path/redpanda.yaml",
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			return fs.MkdirAll("/arbitrary/path", 0755)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := "/arbitrary/path/redpanda.yaml"
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			require.Exactly(st, path, conf.ConfigFile)
		},
	}, {
		name: "it should allow passing arbitrary config values and write them to the config file",
		args: []string{
			"--config", "/arbitrary/path/redpanda.yaml",
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			// --set flags are parsed "outside" of Cobra, directly from
			// os.Args, due to Cobra (or especifically, pflag) parsing
			// list flags (flags that can be passed multiple times) with
			// a CSV parser. Since JSON-formatted values contain commas,
			// the parser doesn't support them.
			os.Args = append(
				os.Args,
				// A single int value
				"--set", "redpanda.node_id=39",
				// A single bool value
				"--set", "rpk.enable_usage_stats=true",
				// A single string value
				"--set", "node_uuid=helloimauuid1337",
				// A JSON object
				"--set", `redpanda.admin=[{"address": "192.168.54.2","port": 9643}]`,
				// A YAML object
				"--set", `redpanda.kafka_api=- name: external
  address: 192.168.73.45
  port: 9092
- name: internal
  address: 10.21.34.58
  port: 9092
`,
			)
			return fs.MkdirAll("/arbitrary/path", 0755)
		},
		after: func() {
			for i, a := range os.Args {
				if a == "--set" {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := "/arbitrary/path/redpanda.yaml"
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			expectedAdmin := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.54.2",
					Port:    9643,
				},
			}}
			expectedKafkaApi := []config.NamedSocketAddress{{
				Name: "external",
				SocketAddress: config.SocketAddress{
					Address: "192.168.73.45",
					Port:    9092,
				},
			}, {
				Name: "internal",
				SocketAddress: config.SocketAddress{
					Address: "10.21.34.58",
					Port:    9092,
				},
			}}
			require.Exactly(st, 39, conf.Redpanda.Id)
			require.Exactly(st, expectedAdmin, conf.Redpanda.AdminApi)
			require.Exactly(st, expectedKafkaApi, conf.Redpanda.KafkaApi)
		},
	}, {
		name: "it should still save values passed through field-specific flags, and prioritize them if they overlap with values set with --set",
		args: []string{
			"--config", "/arbitrary/path/redpanda.yaml",
			"--install-dir", "/var/lib/redpanda",
			// Field-specific flags
			"--advertise-kafka-addr", "plaintext://192.168.34.32:9092",
			"--node-id", "42",
		},
		before: func(fs afero.Fs) error {
			// --set flags are parsed "outside" of Cobra, directly from
			// os.Args, due to Cobra (or especifically, pflag) parsing
			// list flags (flags that can be passed multiple times) with
			// a CSV parser. Since JSON-formatted values contain commas,
			// the parser doesn't support them.
			os.Args = append(
				os.Args,
				// A single int value
				"--set", "redpanda.node_id=39",
				// A single bool value
				"--set", "rpk.enable_usage_stats=true",
				// A single string value
				"--set", "node_uuid=helloimauuid1337",
				// A JSON object
				"--set", `redpanda.admin=[{"address": "192.168.54.2","port": 9643}]`,
				// A YAML object
				"--set", `redpanda.kafka_api=- name: external
  address: 192.168.73.45
  port: 9092
- name: internal
  address: 10.21.34.58
  port: 9092
`,
			)
			return fs.MkdirAll("/arbitrary/path", 0755)
		},
		after: func() {
			for i, a := range os.Args {
				if a == "--set" {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := "/arbitrary/path/redpanda.yaml"
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			expectedAdmin := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.54.2",
					Port:    9643,
				},
			}}
			expectedKafkaApi := []config.NamedSocketAddress{{
				Name: "external",
				SocketAddress: config.SocketAddress{
					Address: "192.168.73.45",
					Port:    9092,
				},
			}, {
				Name: "internal",
				SocketAddress: config.SocketAddress{
					Address: "10.21.34.58",
					Port:    9092,
				},
			}}
			expectedAdvKafkaApi := []config.NamedSocketAddress{{
				Name: "plaintext",
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    9092,
				},
			}}
			// The value set with --node-id should have been prioritized
			require.Exactly(st, 42, conf.Redpanda.Id)
			require.Exactly(st, expectedAdmin, conf.Redpanda.AdminApi)
			require.Exactly(st, expectedKafkaApi, conf.Redpanda.KafkaApi)
			require.Exactly(st, expectedAdvKafkaApi, conf.Redpanda.AdvertisedKafkaApi)
		},
	}, {
		name: "it should evaluate config sources in this order: 1. config file, 2. key-value pairs passed with --set, 3. env vars, 4. specific flags",
		args: []string{
			"--config", "/arbitrary/path/redpanda.yaml",
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "flag://192.168.34.3:9093",
		},
		before: func(fs afero.Fs) error {
			os.Args = append(
				os.Args,
				"--set", `redpanda.kafka_api=- name: set
  address: 192.168.34.2
  port: 9092
`,
			)
			return os.Setenv("REDPANDA_KAFKA_ADDRESS", "env://192.168.34.1:9091")
		},
		after: func() {
			for i, a := range os.Args {
				if a == "--set" {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := "/arbitrary/path/redpanda.yaml"
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			// The value set through the --kafka-addr flag should
			// have been picked.
			expectedKafkaApi := []config.NamedSocketAddress{{
				Name: "flag",
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.3",
					Port:    9093,
				},
			}}
			// The value set with --kafka-addr should have been prioritized
			require.Exactly(st, expectedKafkaApi, conf.Redpanda.KafkaApi)
		},
	}, {
		name: "it should write the default config file path if --config" +
			" isn't passed and the config file doesn't exist",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			require.Exactly(st, config.Default().ConfigFile, conf.ConfigFile)
		},
	}, {
		name: "it should leave config_file untouched if --config wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			require.Exactly(st, config.Default().ConfigFile, conf.ConfigFile)
		},
	}, {
		name: "it should write the given node ID",
		args: []string{
			"--node-id", "34",
			"--config", config.Default().ConfigFile,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			require.Exactly(st, 34, conf.Redpanda.Id)
		},
	}, {
		name: "it should write the default node ID if --node-id isn't passed and the config file doesn't exist",
		args: []string{
			"--config", config.Default().ConfigFile,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, config.Default().Redpanda.Id, conf.Redpanda.Id)
		},
	}, {
		name: "it should leave redpanda.node_id untouched if --node-id wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Redpanda.Id = 98
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			require.Exactly(
				st,
				98,
				conf.Redpanda.Id,
			)
		},
	}, {
		name: "--well-known-io should override rpk.well_known_io",
		args: []string{
			"--well-known-io", "aws:i3xlarge:default",
			"--config", config.Default().ConfigFile,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			require.Exactly(st, "aws:i3xlarge:default", conf.Rpk.WellKnownIo)
		},
	}, {
		name: "it should leave rpk.well_known_io untouched if --well-known-io" +
			" wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.WellKnownIo = "gcp:n2standard:ssd"
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			require.Exactly(
				st,
				"gcp:n2standard:ssd",
				conf.Rpk.WellKnownIo,
			)
		},
	}, {
		name: "--overprovisioned should override the default value for rpk.overprovisioned",
		args: []string{
			// Bool flags will be true by just being present. Therefore, to
			// change their value, <flag>=<value> needs to be used
			"--overprovisioned=false",
			"--config", config.Default().ConfigFile,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, false, conf.Rpk.Overprovisioned)
		},
	}, {
		name: "it should leave rpk.overprovisioned untouched if --overprovisioned wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				config.Default().Rpk.Overprovisioned,
				conf.Rpk.Overprovisioned,
			)
		},
	}, {
		name: "--lock-memory should override the default value for rpk.enable_memory_locking",
		args: []string{
			"--lock-memory",
			"--config", config.Default().ConfigFile,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			path := config.Default().ConfigFile
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(path)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, true, conf.Rpk.EnableMemoryLocking)
		},
	}, {
		name: "it should leave rpk.enable_memory_locking untouched if" +
			" --lock-memory wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.EnableMemoryLocking = true
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				true,
				conf.Rpk.EnableMemoryLocking,
			)
		},
	}, {
		name: "it should parse the --seeds and persist them",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--seeds", "192.168.34.32:33145,somehost:54321,justahostnoport",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedSeeds := []config.SeedServer{{
				Host: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    33145,
				},
			}, {
				Host: config.SocketAddress{
					Address: "somehost",
					Port:    54321,
				},
			}, {
				Host: config.SocketAddress{
					Address: "justahostnoport",
					Port:    33145,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedSeeds,
				conf.Redpanda.SeedServers,
			)
		},
	}, {
		name: "it should parse the --seeds and persist them (shorthand)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"-s", "192.168.3.32:33145",
			"-s", "192.168.123.32:33146,host",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedSeeds := []config.SeedServer{{
				Host: config.SocketAddress{
					Address: "192.168.3.32",
					Port:    33145,
				},
			}, {
				Host: config.SocketAddress{
					Address: "192.168.123.32",
					Port:    33146,
				},
			}, {
				Host: config.SocketAddress{
					Address: "host",
					Port:    33145,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedSeeds,
				conf.Redpanda.SeedServers,
			)
		},
	}, {
		name: "if --seeds wasn't passed, it should fall back to REDPANDA_SEEDS and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(_ afero.Fs) error {
			os.Setenv("REDPANDA_SEEDS", "10.23.12.5:33146,host")
			return nil
		},
		after: func() {
			os.Unsetenv("REDPANDA_SEEDS")
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedSeeds := []config.SeedServer{{
				Host: config.SocketAddress{
					Address: "10.23.12.5",
					Port:    33146,
				},
			}, {
				Host: config.SocketAddress{
					Address: "host",
					Port:    33145,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedSeeds,
				conf.Redpanda.SeedServers,
			)
		},
	}, {
		name: "it should leave existing seeds untouched if --seeds or REDPANDA_SEEDS aren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Redpanda.SeedServers = []config.SeedServer{{
				Host: config.SocketAddress{
					Address: "10.23.12.5",
					Port:    33146,
				},
			}}
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedSeeds := []config.SeedServer{{
				Host: config.SocketAddress{
					Address: "10.23.12.5",
					Port:    33146,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedSeeds,
				conf.Redpanda.SeedServers,
			)
		},
	}, {
		name: "it should fail if the host is missing in the given seed",
		args: []string{
			"-s", "goodhost.com:54897,:33145",
		},
		expectedErrMsg: "Couldn't parse seed ':33145': invalid host \":33145\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
	}, {
		name: "it should fail if the port isnt an int",
		args: []string{
			"-s", "host:port",
		},
		expectedErrMsg: "Couldn't parse seed 'host:port': invalid host \"host:port\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
	}, {
		name: "it should parse the --rpc-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--rpc-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should parse the --rpc-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--rpc-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should fail if --rpc-addr is invalid",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--rpc-addr", "host:nonnumericport",
		},
		expectedErrMsg: "invalid host \"host:nonnumericport\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
	}, {
		name: "if --rpc-addr wasn't passed, it should fall back to REDPANDA_RPC_ADDRESS and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(_ afero.Fs) error {
			os.Setenv("REDPANDA_RPC_ADDRESS", "host:3123")
			return nil
		},
		after: func() {
			os.Unsetenv("REDPANDA_RPC_ADDRESS")
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "host",
				Port:    3123,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should leave the RPC addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Redpanda.RPCServer = config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    33145,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.KafkaApi,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    9092,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.KafkaApi,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it (named)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "nondefaultname://192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Name: "nondefaultname",
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    9092,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.KafkaApi,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it (list)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "nondefaultname://192.168.34.32,host:9092",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Name: "nondefaultname",
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    9092,
				},
			}, {
				SocketAddress: config.SocketAddress{
					Address: "host",
					Port:    9092,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.KafkaApi,
			)
		},
	}, {
		name: "it should fail if --kafka-addr is invalid",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "host:nonnumericport",
		},
		expectedErrMsg: "invalid host \"host:nonnumericport\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
	}, {
		name: "if --kafka-addr wasn't passed, it should fall back to REDPANDA_KAFKA_ADDRESS and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(_ afero.Fs) error {
			os.Setenv("REDPANDA_KAFKA_ADDRESS", "host:3123")
			return nil
		},
		after: func() {
			os.Unsetenv("REDPANDA_KAFKA_ADDRESS")
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "host",
					Port:    3123,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.KafkaApi,
			)
		},
	}, {
		name: "it should leave the Kafka addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Redpanda.KafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.33.33",
					Port:    9892,
				},
			}}
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.33.33",
					Port:    9892,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.KafkaApi,
			)
		},
	}, {
		name: "it should parse the --advertise-kafka-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-kafka-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    33145,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedKafkaApi,
			)
		},
	}, {
		name: "it should parse the --advertise-kafka-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-kafka-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    9092,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedKafkaApi,
			)
		},
	}, {
		name: "it should fail if --advertise-kafka-addr is invalid",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-kafka-addr", "host:nonnumericport",
		},
		expectedErrMsg: "invalid host \"host:nonnumericport\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
	}, {
		name: "if --advertise-kafka-addr, it should fall back to REDPANDA_ADVERTISE_KAFKA_ADDRESS and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(_ afero.Fs) error {
			os.Setenv("REDPANDA_ADVERTISE_KAFKA_ADDRESS", "host:3123")
			return nil
		},
		after: func() {
			os.Unsetenv("REDPANDA_ADVERTISE_KAFKA_ADDRESS")
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "host",
					Port:    3123,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedKafkaApi,
			)
		},
	}, {
		name: "it should leave the adv. Kafka addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Redpanda.AdvertisedKafkaApi = []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.33.33",
					Port:    9892,
				},
			}}
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.33.33",
					Port:    9892,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedKafkaApi,
			)
		},
	}, {
		name: "it should parse the --advertise-pandaproxy-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-pandaproxy-addr", "192.168.34.32:8083",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "192.168.34.32",
					Port:    8083,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Pandaproxy.AdvertisedPandaproxyAPI,
			)
		},
	}, {
		name: "if --advertise-pandaproxy-addr, it should fall back to REDPANDA_ADVERTISE_PANDAPROXY_ADDRESS and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(_ afero.Fs) error {
			os.Setenv("REDPANDA_ADVERTISE_PANDAPROXY_ADDRESS", "host:3123")
			return nil
		},
		after: func() {
			os.Unsetenv("REDPANDA_ADVERTISE_PANDAPROXY_ADDRESS")
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				SocketAddress: config.SocketAddress{
					Address: "host",
					Port:    3123,
				},
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Pandaproxy.AdvertisedPandaproxyAPI,
			)
		},
	}, {
		name: "it should parse the --advertise-rpc-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-rpc-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should parse the --advertise-rpc-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-rpc-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should fail if --advertise-rpc-addr is invalid",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-rpc-addr", "host:nonnumericport",
		},
		expectedErrMsg: "invalid host \"host:nonnumericport\" does not match \"host\", nor \"host:port\", nor \"scheme://host:port\"",
	}, {
		name: "if --advertise-rpc-addr wasn't passed, it should fall back to REDPANDA_ADVERTISE_RPC_ADDRESS and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(_ afero.Fs) error {
			os.Setenv("REDPANDA_ADVERTISE_RPC_ADDRESS", "host:3123")
			return nil
		},
		after: func() {
			os.Unsetenv("REDPANDA_ADVERTISE_RPC_ADDRESS")
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "host",
				Port:    3123,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should leave the adv. RPC addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Redpanda.AdvertisedRPCAPI = &config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			return mgr.Write(conf)
		},
		postCheck: func(fs afero.Fs, _ *rp.RedpandaArgs, st *testing.T) {
			mgr := config.NewManager(fs)
			conf, err := mgr.Read(config.Default().ConfigFile)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				conf.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should fail if --overprovisioned is set in the config file too",
		args: []string{
			"--install-dir", "/var/lib/redpanda", "--overprovisioned",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.AdditionalStartFlags = []string{"--overprovisioned"}
			return mgr.Write(conf)
		},
		expectedErrMsg: "Configuration conflict. Flag '--overprovisioned' is also present in 'rpk.additional_start_flags' in configuration file '/etc/redpanda/redpanda.yaml'. Please remove it and pass '--overprovisioned' directly to `rpk start`.",
	}, {
		name: "it should fail if --smp is set in the config file too",
		args: []string{
			"--install-dir", "/var/lib/redpanda", "--smp", "1",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.AdditionalStartFlags = []string{"--smp=1"}
			return mgr.Write(conf)
		},
		expectedErrMsg: "Configuration conflict. Flag '--smp' is also present in 'rpk.additional_start_flags' in configuration file '/etc/redpanda/redpanda.yaml'. Please remove it and pass '--smp' directly to `rpk start`.",
	}, {
		name: "it should fail if --memory is set in the config file too",
		args: []string{
			"--install-dir", "/var/lib/redpanda", "--memory", "2G",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.AdditionalStartFlags = []string{"--memory=1G"}
			return mgr.Write(conf)
		},
		expectedErrMsg: "Configuration conflict. Flag '--memory' is also present in 'rpk.additional_start_flags' in configuration file '/etc/redpanda/redpanda.yaml'. Please remove it and pass '--memory' directly to `rpk start`.",
	}, {
		name: "it should pass the last instance of a duplicate flag set in rpk.additional_start_flags",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.AdditionalStartFlags = []string{
				"--smp=3", "--smp=55",
			}
			return mgr.Write(conf)
		},
		postCheck: func(
			_ afero.Fs,
			rpArgs *rp.RedpandaArgs,
			st *testing.T,
		) {
			require.Equal(st, "55", rpArgs.SeastarFlags["smp"])
		},
	}, {
		name: "it should allow setting flags with multiple key=values in rpk.additional_start_flags",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			mgr := config.NewManager(fs)
			conf := config.Default()
			conf.Rpk.AdditionalStartFlags = []string{
				"--logger-log-level=archival=debug:cloud_storage=debug",
			}
			return mgr.Write(conf)
		},
		postCheck: func(
			_ afero.Fs,
			rpArgs *rp.RedpandaArgs,
			st *testing.T,
		) {
			require.Equal(st, "archival=debug:cloud_storage=debug", rpArgs.SeastarFlags["logger-log-level"])
		},
	}, {
		name: "it should pass the last instance of a duplicate flag passed to rpk start",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--memory", "1G", "--memory", "4G",
		},
		postCheck: func(
			_ afero.Fs,
			rpArgs *rp.RedpandaArgs,
			st *testing.T,
		) {
			require.Equal(st, "4G", rpArgs.SeastarFlags["memory"])
		},
	}, {
		name: "it should allow arbitrary flags",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--this-flag-is-definitely-not-known", "right",
			"--kernel-page-cache", "1",
			"--another-arbitrary-seastar-flag", "",
		},
	}, {
		name: "it should allow arbitrary flags after '--'",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--",
			"--i-just-made-this-on-the-spot", "nice",
		},
		postCheck: func(
			_ afero.Fs,
			rpArgs *rp.RedpandaArgs,
			st *testing.T,
		) {
			expected := []string{
				"--i-just-made-this-on-the-spot", "nice",
			}
			require.Equal(st, expected, rpArgs.ExtraArgs)
		},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.after != nil {
				defer tt.after()
			}
			fs := afero.NewMemMapFs()
			mgr := config.NewManager(fs)
			var launcher rp.Launcher = &noopLauncher{}
			if tt.launcher != nil {
				launcher = tt.launcher
			}
			if tt.before != nil {
				require.NoError(st, tt.before(fs))
			}
			var out bytes.Buffer
			logrus.SetOutput(&out)
			c := NewStartCommand(fs, mgr, launcher)
			c.SetArgs(tt.args)
			err := c.Execute()
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.NoError(st, err)
			if tt.postCheck != nil {
				l := launcher.(*noopLauncher)
				tt.postCheck(fs, l.rpArgs, st)
			}
		})
	}
}

func TestExtraFlags(t *testing.T) {
	tests := []struct {
		name     string
		flagSet  func() *pflag.FlagSet
		args     []string
		expected map[string]string
	}{{
		name: "it should only return unknown flags",
		flagSet: func() *pflag.FlagSet {
			fset := pflag.NewFlagSet("test", 0)
			_ = fset.Int("int-flag", 0, "usage")
			_ = fset.String("str-flag", "default value", "usage")
			_ = fset.BoolP("bool-flag", "b", true, "usage")
			return fset
		},
		args: []string{
			"--int-flag", "23",
			"--str-flag", "hello",
			"--bool-flag", "false",
			"--kernel-page-cache", "1",
			"--another-arbitrary-seastar-flag", "",
		},
		expected: map[string]string{
			"kernel-page-cache":              "1",
			"another-arbitrary-seastar-flag": "",
		},
	}, {
		name: "it should return an empty map if there are no unknown flags",
		flagSet: func() *pflag.FlagSet {
			fset := pflag.NewFlagSet("test", 0)
			_ = fset.Int("int-flag", 0, "usage")
			_ = fset.String("str-flag", "default value", "usage")
			_ = fset.BoolP("bool-flag", "b", true, "usage")
			return fset
		},
		args: []string{
			"--int-flag", "23",
			"--str-flag", "hello",
		},
		expected: map[string]string{},
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			result := extraFlags(tt.flagSet(), tt.args)
			require.Exactly(st, tt.expected, result)
		})
	}
}
