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
	"os"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/redpanda"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/tuners/iotune"
	"github.com/spf13/afero"
	"github.com/spf13/pflag"
	"github.com/stretchr/testify/require"
	"gopkg.in/yaml.v3"
)

const (
	testConfigFlag string = "/arbitrary/path/redpanda.yaml"
	setFlag        string = "--set"
)

type noopLauncher struct {
	rpArgs *redpanda.RedpandaArgs
}

func (l *noopLauncher) Start(_ string, rpArgs *redpanda.RedpandaArgs) error {
	l.rpArgs = rpArgs
	return nil
}

func TestParseNamedAuthNAddress(t *testing.T) {
	authNSasl := "sasl"
	tests := []struct {
		name           string
		arg            string
		expected       config.NamedAuthNSocketAddress
		expectedErrMsg string
	}{
		{
			name:     "it should parse host:port",
			arg:      "host:9092",
			expected: config.NamedAuthNSocketAddress{Address: "host", Port: 9092, Name: ""},
		},
		{
			name:     "it should parse scheme://host:port",
			arg:      "scheme://host:9092",
			expected: config.NamedAuthNSocketAddress{Address: "host", Port: 9092, Name: "scheme"},
		},
		{
			name:     "it should parse host:port|authn",
			arg:      "host:9092|sasl",
			expected: config.NamedAuthNSocketAddress{Address: "host", Port: 9092, Name: "", AuthN: &authNSasl},
		},
		{
			name:     "it should parse scheme://host:port|authn",
			arg:      "scheme://host:9092|sasl",
			expected: config.NamedAuthNSocketAddress{Address: "host", Port: 9092, Name: "scheme", AuthN: &authNSasl},
		},
		{
			name:           "it should fail for multiple |",
			arg:            "host|sasl|ignore",
			expected:       config.NamedAuthNSocketAddress{},
			expectedErrMsg: `invalid format for listener, at most one "|" can be present: "host|sasl|ignore"`,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			res, err := parseNamedAuthNAddress(tt.arg, 19092)
			if tt.expectedErrMsg != "" {
				require.EqualError(st, err, tt.expectedErrMsg)
				return
			}
			require.Exactly(st, tt.expected, *res)
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
					Host: config.SocketAddress{Address: "127.0.0.1", Port: 1234},
				},
				{
					Host: config.SocketAddress{Address: "domain.com", Port: 9892},
				},
				{
					Host: config.SocketAddress{Address: "lonely-host", Port: 33145},
				},
				{
					Host: config.SocketAddress{Address: "192.168.34.1", Port: 33145},
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
	authNSasl := "sasl"
	tests := []struct {
		name           string
		launcher       redpanda.Launcher
		args           []string
		before         func(afero.Fs) error
		after          func()
		postCheck      func(afero.Fs, *redpanda.RedpandaArgs, *testing.T)
		expectedErrMsg string
	}{{
		name: "should fail if the config at the given path is corrupt",
		args: []string{"--config", config.DefaultRedpandaYamlPath},
		before: func(fs afero.Fs) error {
			return afero.WriteFile(
				fs,
				config.DefaultRedpandaYamlPath,
				[]byte("^&notyaml"),
				0o755,
			)
		},
		expectedErrMsg: "unable to load config file: unable to yaml decode /etc/redpanda/redpanda.yaml: yaml: unmarshal errors:\n  line 1: cannot unmarshal !!str `^&notyaml`",
	}, {
		name: "should generate the config at the given path if it doesn't exist",
		args: []string{
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			path := config.DefaultRedpandaYamlPath
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
			c := config.DevDefault()
			// We are adding now this cluster properties as default with
			// redpanda.developer_mode: true.
			c.Redpanda.Other = map[string]interface{}{
				"auto_create_topics_enabled":    true,
				"group_topic_partitions":        3,
				"storage_min_free_bytes":        10485760,
				"topic_partitions_per_shard":    1000,
				"fetch_reads_debounce_timeout":  10,
				"group_initial_rebalance_delay": 0,
				"log_segment_size_min":          1,
				"write_caching_default":         "true",
			}
			expYAML, err := yaml.Marshal(c)
			require.NoError(st, err)

			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)

			require.YAMLEq(st, string(expYAML), string(y.RawFile()))
		},
	}, {
		name: "it should write the given config file path",
		args: []string{
			"--config", "/arbitrary/path/redpanda.yaml",
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			return fs.MkdirAll("/arbitrary/path", 0o755)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			path := testConfigFlag
			p := &config.Params{ConfigFlag: "/arbitrary/path/redpanda.yaml"} // In command execution this will be done by with ParamsFromCommand
			y, err := p.LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(st, path, y.FileLocation())
			exists, err := afero.Exists(fs, testConfigFlag)
			require.NoError(st, err)
			require.True(st, exists)
		},
	}, {
		name: "it should avoid rewrite if there were no changes in the config file",
		args: []string{
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
			"--advertise-kafka-addr", "plaintext://192.168.34.32:9092",
		},
		before: func(fs afero.Fs) error {
			// The following configuration file is shifted from what rpk
			// would write. We want to verify that running the command
			// 'redpanda start' does not change the contents of the config
			// file , as 'rpk' does not overwrite existing files with
			// identical content.
			return afero.WriteFile(
				fs,
				config.DefaultRedpandaYamlPath,
				[]byte(`redpanda:
    seed_servers: []
    data_directory: /var/lib/redpanda/data
    advertised_kafka_api:
        - address: 192.168.34.32
          name: plaintext
          port: 9092
`),
				0o755,
			)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			file, err := afero.ReadFile(fs, config.DefaultRedpandaYamlPath)
			require.NoError(st, err)
			require.Equal(st, `redpanda:
    seed_servers: []
    data_directory: /var/lib/redpanda/data
    advertised_kafka_api:
        - address: 192.168.34.32
          name: plaintext
          port: 9092
`, string(file))
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
				"--set", "rpk.tune_network=true",
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
			return fs.MkdirAll("/arbitrary/path", 0o755)
		},
		after: func() {
			for i, a := range os.Args {
				if a == setFlag {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			p := &config.Params{ConfigFlag: "/arbitrary/path/redpanda.yaml"}
			y, err := p.LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAdmin := []config.NamedSocketAddress{{
				Address: "192.168.54.2",
				Port:    9643,
			}}
			expectedKafkaAPI := []config.NamedAuthNSocketAddress{{
				Name:    "external",
				Address: "192.168.73.45",
				Port:    9092,
			}, {
				Name:    "internal",
				Address: "10.21.34.58",
				Port:    9092,
			}}
			require.Exactly(st, 39, *y.Redpanda.ID)
			require.Exactly(st, expectedAdmin, y.Redpanda.AdminAPI)
			require.Exactly(st, expectedKafkaAPI, y.Redpanda.KafkaAPI)
			require.Exactly(st, true, y.Rpk.Tuners.TuneNetwork)
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
				"--set", "rpk.tune_cpu=true",
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
			return fs.MkdirAll("/arbitrary/path", 0o755)
		},
		after: func() {
			for i, a := range os.Args {
				if a == setFlag {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			p := &config.Params{ConfigFlag: "/arbitrary/path/redpanda.yaml"}
			y, err := p.LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAdmin := []config.NamedSocketAddress{{
				Address: "192.168.54.2",
				Port:    9643,
			}}
			expectedKafkaAPI := []config.NamedAuthNSocketAddress{{
				Name:    "external",
				Address: "192.168.73.45",
				Port:    9092,
			}, {
				Name:    "internal",
				Address: "10.21.34.58",
				Port:    9092,
			}}
			expectedAdvKafkaAPI := []config.NamedSocketAddress{{
				Name:    "plaintext",
				Address: "192.168.34.32",
				Port:    9092,
			}}
			// The value set with --node-id should have been prioritized
			require.Exactly(st, 42, *y.Redpanda.ID)
			require.Exactly(st, expectedAdmin, y.Redpanda.AdminAPI)
			require.Exactly(st, expectedKafkaAPI, y.Redpanda.KafkaAPI)
			require.Exactly(st, expectedAdvKafkaAPI, y.Redpanda.AdvertisedKafkaAPI)
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
				if a == setFlag {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			p := &config.Params{ConfigFlag: "/arbitrary/path/redpanda.yaml"}
			y, err := p.LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// The value set through the --kafka-addr flag should
			// have been picked.
			expectedKafkaAPI := []config.NamedAuthNSocketAddress{{
				Name:    "flag",
				Address: "192.168.34.3",
				Port:    9093,
			}}
			// The value set with --kafka-addr should have been prioritized
			require.Exactly(st, expectedKafkaAPI, y.Redpanda.KafkaAPI)
		},
	}, {
		name: "it should write the default config file path if --config" +
			" isn't passed and the config file doesn't exist",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(st, config.DefaultRedpandaYamlPath, y.FileLocation())
		},
	}, {
		name: "it should leave config_file untouched if --config wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y := config.DevDefault()
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(st, config.DefaultRedpandaYamlPath, y.FileLocation())
		},
	}, {
		name: "it should write the given node ID",
		args: []string{
			"--node-id", "34",
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(st, 34, *y.Redpanda.ID)
		},
	}, {
		name: "it should write the default node ID if --node-id isn't passed and the config file doesn't exist",
		args: []string{
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, config.DevDefault().Redpanda.ID, y.Redpanda.ID)
		},
	}, {
		name: "it should write default data_directory if loaded config doesn't have one",
		args: []string{
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y := config.DevDefault()
			y.Redpanda.Directory = ""
			return y.Write(fs)
		},
		postCheck: func(
			fs afero.Fs,
			_ *redpanda.RedpandaArgs,
			st *testing.T,
		) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, config.DevDefault().Redpanda.Directory, y.Redpanda.Directory)
		},
	}, {
		name: "it should leave redpanda.node_id untouched if --node-id wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.ID = new(int)
			*y.Redpanda.ID = 98
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(
				st,
				98,
				*y.Redpanda.ID,
			)
		},
	}, {
		name: "--well-known-io should override rpk.well_known_io",
		args: []string{
			"--well-known-io", "aws:i3xlarge:default",
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(st, "aws:i3xlarge:default", y.Rpk.Tuners.WellKnownIo)
		},
	}, {
		name: "it should leave rpk.well_known_io untouched if --well-known-io" +
			" wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.Tuners.WellKnownIo = "gcp:n2standard:ssd"
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Exactly(
				st,
				"gcp:n2standard:ssd",
				y.Rpk.Tuners.WellKnownIo,
			)
		},
	}, {
		name: "--overprovisioned should override the default value for rpk.overprovisioned",
		args: []string{
			// Bool flags will be true by just being present. Therefore, to
			// change their value, <flag>=<value> needs to be used
			"--overprovisioned=false",
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, false, y.Rpk.Overprovisioned)
		},
	}, {
		name: "it should leave rpk.overprovisioned untouched if --overprovisioned wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y := config.DevDefault()
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				config.DevDefault().Rpk.Overprovisioned,
				y.Rpk.Overprovisioned,
			)
		},
	}, {
		name: "--lock-memory should override the default value for rpk.enable_memory_locking",
		args: []string{
			"--lock-memory",
			"--config", config.DefaultRedpandaYamlPath,
			"--install-dir", "/var/lib/redpanda",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(st, true, y.Rpk.EnableMemoryLocking)
		},
	}, {
		name: "it should leave rpk.enable_memory_locking untouched if" +
			" --lock-memory wasn't passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.EnableMemoryLocking = true
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				true,
				y.Rpk.EnableMemoryLocking,
			)
		},
	}, {
		name: "it should parse the --seeds and persist them",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--seeds", "192.168.34.32:33145,somehost:54321,justahostnoport",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
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
				y.Redpanda.SeedServers,
			)
		},
	}, {
		name: "it should parse the --seeds and persist them (shorthand)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"-s", "192.168.3.32:33145",
			"-s", "192.168.123.32:33146,host",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
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
				y.Redpanda.SeedServers,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
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
				y.Redpanda.SeedServers,
			)
		},
	}, {
		name: "it should leave existing seeds untouched if --seeds or REDPANDA_SEEDS aren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.SeedServers = []config.SeedServer{{
				Host: config.SocketAddress{
					Address: "10.23.12.5",
					Port:    33146,
				},
			}}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
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
				y.Redpanda.SeedServers,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should parse the --rpc-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--rpc-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.RPCServer,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "host",
				Port:    3123,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should leave the RPC addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.RPCServer = config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.RPCServer,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedAuthNSocketAddress{{
				Address: "192.168.34.32",
				Port:    33145,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.KafkaAPI,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedAuthNSocketAddress{{
				Address: "192.168.34.32",
				Port:    9092,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.KafkaAPI,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it (named)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "nondefaultname://192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedAuthNSocketAddress{{
				Name:    "nondefaultname",
				Address: "192.168.34.32",
				Port:    9092,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.KafkaAPI,
			)
		},
	}, {
		name: "it should parse the --kafka-addr and persist it (list)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--kafka-addr", "nondefaultname://192.168.34.32,host:9092,authn://host:9093|sasl",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedAuthNSocketAddress{{
				Name:    "nondefaultname",
				Address: "192.168.34.32",
				Port:    9092,
			}, {
				Address: "host",
				Port:    9092,
			}, {
				Name:    "authn",
				Address: "host",
				Port:    9093,
				AuthN:   &authNSasl,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.KafkaAPI,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedAuthNSocketAddress{{
				Address: "host",
				Port:    3123,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.KafkaAPI,
			)
		},
	}, {
		name: "it should leave the Kafka addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.KafkaAPI = []config.NamedAuthNSocketAddress{{
				Address: "192.168.33.33",
				Port:    9892,
			}}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedAuthNSocketAddress{{
				Address: "192.168.33.33",
				Port:    9892,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.KafkaAPI,
			)
		},
	}, {
		name: "it should parse the --advertise-kafka-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-kafka-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Address: "192.168.34.32",
				Port:    33145,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedKafkaAPI,
			)
		},
	}, {
		name: "it should parse the --advertise-kafka-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-kafka-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Address: "192.168.34.32",
				Port:    9092,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedKafkaAPI,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Address: "host",
				Port:    3123,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedKafkaAPI,
			)
		},
	}, {
		name: "it should leave the adv. Kafka addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.AdvertisedKafkaAPI = []config.NamedSocketAddress{{
				Address: "192.168.33.33",
				Port:    9892,
			}}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Address: "192.168.33.33",
				Port:    9892,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedKafkaAPI,
			)
		},
	}, {
		name: "it should leave cfg_file.pandaproxy untouched if no pandaproxy flags are passed",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y := config.DevDefault()
			y.Pandaproxy = &config.Pandaproxy{
				PandaproxyAPI: []config.NamedAuthNSocketAddress{
					{Address: "127.0.0.1", Port: 8888, Name: "advertised"},
				},
				PandaproxyAPITLS: []config.ServerTLS{
					{Name: "my-tls", KeyFile: "redpanda.key"},
				},
				AdvertisedPandaproxyAPI: []config.NamedSocketAddress{
					{Address: "foo.com", Port: 8888, Name: "advertised"},
				},
				Other: map[string]interface{}{"foo": "bar"},
			}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedPandaProxy := &config.Pandaproxy{
				PandaproxyAPI: []config.NamedAuthNSocketAddress{
					{Address: "127.0.0.1", Port: 8888, Name: "advertised"},
				},
				PandaproxyAPITLS: []config.ServerTLS{
					{Name: "my-tls", KeyFile: "redpanda.key"},
				},
				AdvertisedPandaproxyAPI: []config.NamedSocketAddress{
					{Address: "foo.com", Port: 8888, Name: "advertised"},
				},
				Other: map[string]interface{}{"foo": "bar"},
			}
			require.Exactly(st, y.Pandaproxy, expectedPandaProxy)
		},
	}, {
		name: "it should override cfg_file.pandaproxy.advertised_pandaproxy_api with the --advertise-pandaproxy-addr",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-pandaproxy-addr", "changed://192.168.34.32:8083",
		},
		before: func(fs afero.Fs) error {
			y := config.DevDefault()
			y.Pandaproxy = &config.Pandaproxy{
				PandaproxyAPI: []config.NamedAuthNSocketAddress{
					{Address: "127.0.0.1", Port: 8888, Name: "advertised"},
				},
				PandaproxyAPITLS: []config.ServerTLS{
					{Name: "my-tls", KeyFile: "redpanda.key"},
				},
				AdvertisedPandaproxyAPI: []config.NamedSocketAddress{
					{Address: "foo.com", Port: 8888, Name: "advertised"},
				},
				Other: map[string]interface{}{"foo": "bar"},
			}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			// we compare the whole pandaproxy field to check we are not
			// changing anything else.
			expectedPandaProxy := &config.Pandaproxy{
				PandaproxyAPI: []config.NamedAuthNSocketAddress{
					{Address: "127.0.0.1", Port: 8888, Name: "advertised"},
				},
				PandaproxyAPITLS: []config.ServerTLS{
					{Name: "my-tls", KeyFile: "redpanda.key"},
				},
				AdvertisedPandaproxyAPI: []config.NamedSocketAddress{
					{Address: "192.168.34.32", Port: 8083, Name: "changed"},
				},
				Other: map[string]interface{}{"foo": "bar"},
			}
			require.Exactly(st, y.Pandaproxy, expectedPandaProxy)
		},
	}, {
		name: "it should parse the --advertise-pandaproxy-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-pandaproxy-addr", "192.168.34.32:8083",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Address: "192.168.34.32",
				Port:    8083,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Pandaproxy.AdvertisedPandaproxyAPI,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := []config.NamedSocketAddress{{
				Address: "host",
				Port:    3123,
			}}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Pandaproxy.AdvertisedPandaproxyAPI,
			)
		},
	}, {
		name: "it should parse the --advertise-rpc-addr and persist it",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-rpc-addr", "192.168.34.32:33145",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should parse the --advertise-rpc-addr and persist it (no port)",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--advertise-rpc-addr", "192.168.34.32",
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "192.168.34.32",
				Port:    33145,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedRPCAPI,
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
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "host",
				Port:    3123,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should leave the adv. RPC addr untouched if the env var & flag weren't set",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.AdvertisedRPCAPI = &config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			return y.Write(fs)
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			expectedAddr := &config.SocketAddress{
				Address: "192.168.33.33",
				Port:    9892,
			}
			// Check that the generated config is as expected.
			require.Exactly(
				st,
				expectedAddr,
				y.Redpanda.AdvertisedRPCAPI,
			)
		},
	}, {
		name: "it should fail if --overprovisioned is set in the config file too",
		args: []string{
			"--install-dir", "/var/lib/redpanda", "--overprovisioned",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.AdditionalStartFlags = []string{"--overprovisioned"}
			return y.Write(fs)
		},
		expectedErrMsg: "configuration conflict. Flag '--overprovisioned' is also present in 'rpk.additional_start_flags' in configuration file '/etc/redpanda/redpanda.yaml'. Please remove it and pass '--overprovisioned' directly to `rpk start`",
	}, {
		name: "it should fail if --smp is set in the config file too",
		args: []string{
			"--install-dir", "/var/lib/redpanda", "--smp", "1",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.AdditionalStartFlags = []string{"--smp=1"}
			return y.Write(fs)
		},
		expectedErrMsg: "configuration conflict. Flag '--smp' is also present in 'rpk.additional_start_flags' in configuration file '/etc/redpanda/redpanda.yaml'. Please remove it and pass '--smp' directly to `rpk start`",
	}, {
		name: "it should fail if --memory is set in the config file too",
		args: []string{
			"--install-dir", "/var/lib/redpanda", "--memory", "2G",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.AdditionalStartFlags = []string{"--memory=1G"}
			return y.Write(fs)
		},
		expectedErrMsg: "configuration conflict. Flag '--memory' is also present in 'rpk.additional_start_flags' in configuration file '/etc/redpanda/redpanda.yaml'. Please remove it and pass '--memory' directly to `rpk start`",
	}, {
		name: "it should pass the last instance of a duplicate flag set in rpk.additional_start_flags",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
		},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.AdditionalStartFlags = []string{
				"--smp=3", "--smp=55",
			}
			return y.Write(fs)
		},
		postCheck: func(
			_ afero.Fs,
			rpArgs *redpanda.RedpandaArgs,
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
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Rpk.AdditionalStartFlags = []string{
				"--logger-log-level=archival=debug:cloud_storage=debug",
			}
			return y.Write(fs)
		},
		postCheck: func(
			_ afero.Fs,
			rpArgs *redpanda.RedpandaArgs,
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
			rpArgs *redpanda.RedpandaArgs,
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
			rpArgs *redpanda.RedpandaArgs,
			st *testing.T,
		) {
			expected := []string{
				"--i-just-made-this-on-the-spot", "nice",
			}
			require.Equal(st, expected, rpArgs.ExtraArgs)
		},
	}, {
		name: "--mode dev-container flag set required bundle of flags",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--mode", "dev-container",
		},
		postCheck: func(
			fs afero.Fs,
			rpArgs *redpanda.RedpandaArgs,
			st *testing.T,
		) {
			// Flags:
			require.Equal(st, "true", rpArgs.SeastarFlags["overprovisioned"])
			require.Equal(st, "0M", rpArgs.SeastarFlags["reserve-memory"])
			require.Equal(st, "true", rpArgs.SeastarFlags["unsafe-bypass-fsync"])

			// Config:
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Nil(st, y.Redpanda.ID)
			require.Equal(st, true, y.Redpanda.DeveloperMode)
			expectedClusterFields := map[string]interface{}{
				"auto_create_topics_enabled":    true,
				"group_topic_partitions":        3,
				"storage_min_free_bytes":        10485760,
				"topic_partitions_per_shard":    1000,
				"fetch_reads_debounce_timeout":  10,
				"group_initial_rebalance_delay": 0,
				"log_segment_size_min":          1,
				"write_caching_default":         "true",
			}
			require.Equal(st, expectedClusterFields, y.Redpanda.Other)
		},
	}, {
		name: "override flags set by --mode dev-container",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--mode", "dev-container", "--reserve-memory", "2M",
		},
		postCheck: func(
			fs afero.Fs,
			rpArgs *redpanda.RedpandaArgs,
			st *testing.T,
		) {
			// override value:
			require.Equal(st, "2M", rpArgs.SeastarFlags["reserve-memory"])
			// rest of --mode dev-container bundle
			require.Equal(st, "true", rpArgs.SeastarFlags["overprovisioned"])
			require.Equal(st, "true", rpArgs.SeastarFlags["unsafe-bypass-fsync"])
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)
			require.Nil(st, y.Redpanda.ID)
			require.Equal(st, true, y.Redpanda.DeveloperMode)
		},
	}, {
		name: "redpanda.developer_mode: true behaves like --mode dev-container",
		args: []string{"--install-dir", "/var/lib/redpanda"},
		before: func(fs afero.Fs) error {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			y.Redpanda.DeveloperMode = true
			return y.Write(fs)
		},
		postCheck: func(
			fs afero.Fs,
			rpArgs *redpanda.RedpandaArgs,
			st *testing.T,
		) {
			// Flags:
			require.Equal(st, "true", rpArgs.SeastarFlags["overprovisioned"])
			require.Equal(st, "0M", rpArgs.SeastarFlags["reserve-memory"])
			y, err := new(config.Params).LoadVirtualRedpandaYaml(fs)
			require.NoError(st, err)

			// Config:
			expectedClusterFields := map[string]interface{}{
				"auto_create_topics_enabled":    true,
				"group_topic_partitions":        3,
				"storage_min_free_bytes":        10485760,
				"topic_partitions_per_shard":    1000,
				"fetch_reads_debounce_timeout":  10,
				"group_initial_rebalance_delay": 0,
				"log_segment_size_min":          1,
				"write_caching_default":         "true",
			}
			require.Nil(st, y.Redpanda.ID)
			require.Equal(st, true, y.Redpanda.DeveloperMode)
			require.Equal(st, expectedClusterFields, y.Redpanda.Other)
		},
	}, {
		name: "--set overrides cluster configs set by --mode dev-container",
		args: []string{
			"--install-dir", "/var/lib/redpanda",
			"--mode", "dev-container",
		},
		before: func(fs afero.Fs) error {
			// --set flags are parsed "outside" of Cobra, directly from
			// os.Args.
			os.Args = append(
				os.Args,
				// A single int value
				"--set", "redpanda.auto_create_topics_enabled=false",
				// A single bool value
				"--set", "redpanda.group_topic_partitions=1",
			)
			return nil
		},
		after: func() {
			for i, a := range os.Args {
				if a == setFlag {
					os.Args = os.Args[:i]
					return
				}
			}
		},
		postCheck: func(fs afero.Fs, _ *redpanda.RedpandaArgs, st *testing.T) {
			y, _ := new(config.Params).LoadVirtualRedpandaYaml(fs)
			expectedClusterFields := map[string]interface{}{
				// set by --set flag
				"auto_create_topics_enabled": false,
				"group_topic_partitions":     1,
				// rest of --mode dev-container cfg fields
				"storage_min_free_bytes":        10485760,
				"topic_partitions_per_shard":    1000,
				"fetch_reads_debounce_timeout":  10,
				"group_initial_rebalance_delay": 0,
				"log_segment_size_min":          1,
				"write_caching_default":         "true",
			}
			require.Exactly(st, expectedClusterFields, y.Redpanda.Other)
		},
	}, {
		name:           "Fails if unknown mode is passed",
		args:           []string{"--install-dir", "/var/lib/redpanda", "--mode", "foo"},
		expectedErrMsg: `unrecognized mode "foo"`,
	}}

	for _, tt := range tests {
		t.Run(tt.name, func(st *testing.T) {
			if tt.after != nil {
				defer tt.after()
			}
			fs := afero.NewMemMapFs()
			var launcher redpanda.Launcher = &noopLauncher{}
			if tt.launcher != nil {
				launcher = tt.launcher
			}
			if tt.before != nil {
				require.NoError(st, tt.before(fs))
			}
			p := new(config.Params)
			c := NewStartCommand(fs, p, launcher)
			c.Flags().StringVar(&p.ConfigFlag, "config", "", "this is done in root.go, but we need it here for the tests setting args")
			// We disable --check flag to avoid running tuner checks in Afero's
			// memory backed file system.
			args := append([]string{"--check=false"}, tt.args...)
			c.SetArgs(args)
			err := c.Execute()
			if tt.expectedErrMsg != "" {
				require.Contains(st, err.Error(), tt.expectedErrMsg)
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

func Test_buildRedpandaFlags(t *testing.T) {
	type args struct {
		y          *config.RedpandaYaml
		args       []string
		sFlags     seastarFlags
		flags      map[string]string
		ioResolver func(*config.RedpandaYaml, bool) (*iotune.IoProperties, error)
	}
	tests := []struct {
		name       string
		args       args
		writeIoCfg bool
		exp        map[string]string
		expErr     bool
	}{
		{
			name: "err when ioPropertiesFlag and wellKnownIo are set",
			args: args{
				y:     &config.RedpandaYaml{Rpk: config.RpkNodeConfig{Tuners: config.RpkNodeTuners{WellKnownIo: "some io"}}},
				flags: map[string]string{ioPropertiesFlag: "{some:value}"},
			},
			expErr: true,
		}, {
			name: "err when ioPropertiesFileFlag and wellKnownIo are set",
			args: args{
				y:     &config.RedpandaYaml{Rpk: config.RpkNodeConfig{Tuners: config.RpkNodeTuners{WellKnownIo: "some io"}}},
				flags: map[string]string{ioPropertiesFileFlag: ""},
			},
			expErr: true,
		}, {
			name: "setting the properties from the config file ",
			args: args{
				y: &config.RedpandaYaml{Rpk: config.RpkNodeConfig{
					AdditionalStartFlags: []string{"--abort-on-seastar-bad-alloc=true"},
					Overprovisioned:      true,
					EnableMemoryLocking:  false,
					SMP:                  intPtr(2),
				}},
			},
			exp: map[string]string{
				"overprovisioned":            "true",
				"lock-memory":                "false",
				"smp":                        "2",
				"abort-on-seastar-bad-alloc": "true",
			},
		}, {
			name: "err if flag and additional_start_flags are present",
			args: args{
				flags: map[string]string{maxIoRequestsFlag: "2"},
				y: &config.RedpandaYaml{Rpk: config.RpkNodeConfig{
					AdditionalStartFlags: []string{"--max-io-requests=3"},
				}},
			},
			expErr: true,
		}, {
			name: "get the io property file from the default location",
			args: args{
				y: &config.RedpandaYaml{},
			},
			writeIoCfg: true,
			exp: map[string]string{
				"io-properties-file": "io-config.yaml",
				// These 2 are false because of the empty config
				"overprovisioned": "false",
				"lock-memory":     "false",
			},
		}, {
			name: "get the io property from the  ioResolver",
			args: args{
				y: &config.RedpandaYaml{},
				ioResolver: func(*config.RedpandaYaml, bool) (*iotune.IoProperties, error) {
					return &iotune.IoProperties{
						MountPoint:     "/mnt/",
						ReadIops:       2,
						ReadBandwidth:  3,
						WriteIops:      4,
						WriteBandwidth: 5,
					}, nil
				},
			},
			exp: map[string]string{
				"io-properties": `{"disks":[{"mountpoint":"/mnt/","read_iops":2,"read_bandwidth":3,"write_iops":4,"write_bandwidth":5}]}`,
				// These 2 are false because of the empty config
				"overprovisioned": "false",
				"lock-memory":     "false",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fs := afero.NewMemMapFs()

			if tt.writeIoCfg {
				err := afero.WriteFile(fs, "io-config.yaml", []byte{}, 0o655)
				require.NoError(t, err)
			}

			cmdFlag := NewStartCommand(fs, new(config.Params), &noopLauncher{}).Flags()
			if len(tt.args.flags) > 0 {
				for k, v := range tt.args.flags {
					err := cmdFlag.Set(k, v)
					require.NoError(t, err)
				}
			}

			ioResolver := func(*config.RedpandaYaml, bool) (*iotune.IoProperties, error) {
				return nil, nil
			}
			if tt.args.ioResolver != nil {
				ioResolver = tt.args.ioResolver
			}

			// We can safely pass 'skipCheck:true' to buildRedpandaFlags since
			// this is only used for the ioResolver function, and we are mocking
			// it.
			rpArgs, err := buildRedpandaFlags(fs, tt.args.y, tt.args.args, tt.args.sFlags, cmdFlag, true, ioResolver)
			if tt.expErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)

			// We will just test the output of the built flags, since the other
			// field of the struct is the config file location which we are
			// writing in a MemMap FS in the test
			require.Equal(t, tt.exp, rpArgs.SeastarFlags)
		})
	}
}

func Test_ParseFlags(t *testing.T) {
	for _, tt := range []struct {
		name string
		in   []string
		exp  map[string]string
	}{
		{
			name: "empty flags",
			in:   []string{"", "-", "--"},
			exp:  map[string]string{},
		}, {
			name: "bool flags - no value",
			in:   []string{"--overprovisioned", "-true"},
			exp:  map[string]string{"overprovisioned": "", "true": ""},
		}, {
			name: "flags with value",
			in:   []string{"--smp=2", "--memory=4G", "--default-log-level=info"},
			exp:  map[string]string{"smp": "2", "memory": "4G", "default-log-level": "info"},
		}, {
			name: "flags with value and quotes",
			in:   []string{`--logger-log-level="rpc=debug"`},
			exp:  map[string]string{"logger-log-level": "rpc=debug"},
		},
	} {
		t.Run(tt.name, func(st *testing.T) {
			got := parseFlags(tt.in)
			require.Equal(st, tt.exp, got)
		})
	}
}

func intPtr(i int) *int {
	return &i
}
