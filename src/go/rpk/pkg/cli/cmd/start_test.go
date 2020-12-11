// Copyright 2020 Vectorized, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package cmd

import (
	"testing"
	"vectorized/pkg/config"

	"github.com/stretchr/testify/require"
)

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
			overrides: []string{"--overprovisioned", "--unsafe-bypass-fsync 1",
				"--default-log-level=trace", "--logger-log-level='exception=debug'",
				"--fail-on-abandoned-failed-futures"},
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
			arg:  []string{"127.0.0.1:1234+0", "domain.com:9892+1", "lonely-host+30", "192.168.34.1+5"},
			expected: []config.SeedServer{
				{
					config.SocketAddress{"127.0.0.1", 1234},
					0,
				},
				{
					config.SocketAddress{"domain.com", 9892},
					1,
				},
				{
					config.SocketAddress{"lonely-host", 33145},
					30,
				},
				{
					config.SocketAddress{"192.168.34.1", 33145},
					5,
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
			arg:            []string{"+1"},
			expectedErrMsg: "Couldn't parse seed '+1': empty address",
		},
		{
			name:           "it should fail if one of the addrs is missing an ID",
			arg:            []string{"127.0.0.1:1234+0", "domain.com"},
			expectedErrMsg: "Couldn't parse seed 'domain.com': Format doesn't conform to <host>[:<port>]+<id>. Missing ID.",
		},
		{
			name:           "it should fail if one of the addrs' ID isn't an int",
			arg:            []string{"127.0.0.1:1234+id?", "domain.com+1"},
			expectedErrMsg: "Couldn't parse seed '127.0.0.1:1234+id?': ID must be an int.",
		},
		{
			name:           "it should fail if the host is empty",
			arg:            []string{" :1234+1234"},
			expectedErrMsg: "Couldn't parse seed ' :1234+1234': Empty host in address ' :1234'",
		},
		{
			name:           "it should fail if the port is empty",
			arg:            []string{" :+1234"},
			expectedErrMsg: "Couldn't parse seed ' :+1234': Empty host in address ' :'",
		},
		{
			name:           "it should fail if the port is empty",
			arg:            []string{"host:+1234"},
			expectedErrMsg: "Couldn't parse seed 'host:+1234': Port must be an int",
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
