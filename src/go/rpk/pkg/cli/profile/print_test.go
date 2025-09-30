// Copyright 2025 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

package profile

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

var rpkYamlBytes = []byte(`version: 7
globals:
current_profile: test
profiles:
  - name: test
    kafka_api:
      sasl:
        password: "redactme"
      brokers:
        - 192.168.0.1:9092`)

func saveConfigFile(t *testing.T, fs afero.Fs, buf []byte) {
	// Create a default rpk.yaml file on the provided filesystem with the given contents
	configDir, err := os.UserConfigDir()
	require.NoError(t, err, "error getting os.UserConfigDir")

	configPath := filepath.Join(configDir, "rpk", "rpk.yaml")
	require.NoError(t, afero.WriteFile(fs, configPath, buf, 0o644), "error writing yaml file")
}

func TestPrintProfile(t *testing.T) {
	fs := afero.NewMemMapFs()
	saveConfigFile(t, fs, rpkYamlBytes)

	params := &config.Params{}
	contents, err := renderCommand(fs, params, []string{})
	require.NoError(t, err)
	require.Contains(t, contents, "name: test")
	require.Contains(t, contents, "- 192.168.0.1:9092")
}

func TestPrintProfileVerbose(t *testing.T) {
	// Verbose (-v/--verbose) should print both the config from disk as well as the computed config.
	// Use the rpk.yaml from above, and override the brokers value with an environment variable
	// to make sure both instances show up
	fs := afero.NewMemMapFs()
	saveConfigFile(t, fs, rpkYamlBytes)

	// DebugLogs is set to `true` when `--verbose` is used
	params := &config.Params{DebugLogs: true}

	// Override the brokers value
	t.Setenv("RPK_BROKERS", "example.com:9092,example.org:9092")

	contents, err := renderCommand(fs, params, []string{})
	require.NoError(t, err)

	// The output should contain the value from the file _and_ the value from the environment
	// in separate blocks
	require.Contains(t, contents, "name: test")
	require.Contains(t, contents, defaultFileAnnotation)
	require.Contains(t, contents, "- 192.168.0.1:9092")
	require.Contains(t, contents, effectiveConfigAnnotation)
	require.Contains(t, contents, "- example.com:9092")
	require.Contains(t, contents, "- example.org:9092")

	// We should redact the sasl password in both formats
	require.NotContains(t, contents, "redactme")
}

func TestRenderProfile(t *testing.T) {
	// Test rendering a single profile from a *config.RpkYaml
	config := &config.RpkYaml{
		Profiles: []config.RpkProfile{
			{
				Name: "test",
				LicenseCheck: &config.LicenseStatusCache{
					LastUpdate: 1000,
				},
				KafkaAPI: config.RpkKafkaAPI{
					SASL: &config.SASL{Password: "redactme"},
				},
			},
		},
	}

	_, err := renderProfile(config, "nonexistant")
	require.Error(t, err)

	contents, err := renderProfile(config, "test")
	require.NoError(t, err)
	require.Contains(t, contents, "name: test")
	require.Contains(t, contents, "[REDACTED]")
	require.NotContains(t, contents, "license_check")
	require.NotContains(t, contents, "redactme")
}
