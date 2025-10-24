package profile

import (
	"os"
	"path/filepath"
	"testing"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

// example output from `rpk print profile -v`
const profilePrintVerboseOutput = `--- Loaded from file: rpk.yaml
prompt: file-based

--- Effective configuration
prompt: effective-config`

func TestCombineClusterNames(t *testing.T) {
	tests := []struct {
		name string
		rgs  []*controlplanev1.ResourceGroup
		scs  []*controlplanev1.ServerlessCluster
		cs   []*controlplanev1.Cluster
		exp  namesAndClusters
	}{
		{
			name: "combine Serverless Clusters and Clusters",
			rgs: []*controlplanev1.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
				{Id: "rg2", Name: "ResourceGroup2"},
			},
			scs: []*controlplanev1.ServerlessCluster{
				{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY},
				{ResourceGroupId: "rg2", Name: "SC2", State: controlplanev1.ServerlessCluster_STATE_READY},
				{ResourceGroupId: "rg1", Name: "SC3", State: controlplanev1.ServerlessCluster_STATE_CREATING}, // should not appear if it's not ready.
			},
			cs: []*controlplanev1.Cluster{
				{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY},
				{ResourceGroupId: "rg2", Name: "C2", State: controlplanev1.Cluster_STATE_DELETING}, // should not appear if it's not ready.
				{ResourceGroupId: "rg2", Name: "C3", State: controlplanev1.Cluster_STATE_READY},
			},
			exp: namesAndClusters{
				{name: "ResourceGroup1/SC1", sc: &controlplanev1.ServerlessCluster{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY}},
				{name: "ResourceGroup2/SC2", sc: &controlplanev1.ServerlessCluster{ResourceGroupId: "rg2", Name: "SC2", State: controlplanev1.ServerlessCluster_STATE_READY}},
				{name: "ResourceGroup1/C1", c: &controlplanev1.Cluster{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY}},
				{name: "ResourceGroup2/C3", c: &controlplanev1.Cluster{ResourceGroupId: "rg2", Name: "C3", State: controlplanev1.Cluster_STATE_READY}},
			},
		},
		{
			name: "empty inputs",
			rgs:  []*controlplanev1.ResourceGroup{},
			scs:  []*controlplanev1.ServerlessCluster{},
			cs:   []*controlplanev1.Cluster{},
			exp:  nil,
		},
		{
			name: "nil inputs",
			rgs:  nil,
			scs:  nil,
			cs:   nil,
			exp:  nil,
		},
		{
			name: "Serverless Clusters only",
			rgs: []*controlplanev1.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
			},
			scs: []*controlplanev1.ServerlessCluster{
				{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY},
			},
			cs: []*controlplanev1.Cluster{},
			exp: namesAndClusters{
				{name: "ResourceGroup1/SC1", sc: &controlplanev1.ServerlessCluster{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1.ServerlessCluster_STATE_READY}},
			},
		},
		{
			name: "Clusters only",
			rgs: []*controlplanev1.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
			},
			scs: []*controlplanev1.ServerlessCluster{},
			cs: []*controlplanev1.Cluster{
				{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY},
			},
			exp: namesAndClusters{
				{name: "ResourceGroup1/C1", c: &controlplanev1.Cluster{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1.Cluster_STATE_READY}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := combineClusterNames(tt.rgs, tt.scs, tt.cs)
			require.Equal(t, tt.exp, result)
		})
	}
}

func TestFixFromCloudArgs(t *testing.T) {
	tests := []struct {
		name string
		args []string
		exp  []string
	}{
		{
			name: "no from-cloud flag",
			args: []string{"create", "profile-name", "--from-rpk-container"},
			exp:  []string{"create", "profile-name", "--from-rpk-container"},
		},
		{
			name: "from-cloud with equals syntax",
			args: []string{"--from-cloud=cluster-123", "profile-name"},
			exp:  []string{"--from-cloud=cluster-123", "profile-name"},
		},
		{
			name: "from-cloud with value as separate arg",
			args: []string{"create", "--from-cloud", "cluster-123", "profile-name"},
			exp:  []string{"create", "--from-cloud=cluster-123", "profile-name"},
		},
		{
			name: "from-cloud not at index 0",
			args: []string{"--from-cloud", "cluster-123"},
			exp:  []string{"--from-cloud=cluster-123"},
		},
		{
			name: "from-cloud at end without value",
			args: []string{"extra-arg", "profile-name", "--from-cloud"},
			exp:  []string{"extra-arg", "profile-name", "--from-cloud"},
		},
		{
			name: "from-cloud as only arg",
			args: []string{"--from-cloud"},
			exp:  []string{"--from-cloud"},
		},
		{
			name: "from-cloud with value as separate arg - 2nd arg",
			args: []string{"create", "profile-name", "--from-cloud", "cluster-123"},
			exp:  []string{"create", "profile-name", "--from-cloud=cluster-123"},
		},
		{
			name: "from-cloud with cluster ID followed by another flag",
			args: []string{"create", "--from-cloud", "cluster-123", "--verbose", "profile-name"},
			exp:  []string{"create", "--from-cloud=cluster-123", "--verbose", "profile-name"},
		},
		{
			name: "from-cloud followed by another flag (no cluster ID)",
			args: []string{"create", "--from-cloud", "--verbose", "profile-name"},
			exp:  []string{"create", "--from-cloud", "--verbose", "profile-name"},
		},
		{
			name: "multiple from-cloud flags - last one wins",
			args: []string{"create", "--from-cloud", "cluster-111", "--from-cloud", "cluster-222", "profile-name"},
			exp:  []string{"create", "--from-cloud", "cluster-111", "--from-cloud=cluster-222", "profile-name"},
		},
		{
			name: "from-cloud as first arg with value",
			args: []string{"--from-cloud", "cluster-123", "create", "profile-name"},
			exp:  []string{"--from-cloud=cluster-123", "create", "profile-name"},
		},
		{
			name: "from-cloud with hyphenated cluster ID",
			args: []string{"create", "--from-cloud", "cluster-abc-123", "profile-name"},
			exp:  []string{"create", "--from-cloud=cluster-abc-123", "profile-name"},
		},
		{
			name: "empty args slice",
			args: []string{},
			exp:  []string{},
		},
		{
			name: "from-cloud with value containing spaces (edge case, not possible in practice rn)",
			args: []string{"create", "--from-cloud", "cluster with spaces", "profile-name"},
			exp:  []string{"create", "--from-cloud=cluster with spaces", "profile-name"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := fixFromCloudArgs(tt.args)
			require.Equal(t, tt.exp, result)
		})
	}
}

func TestCreateFromProfile(t *testing.T) {
	// We should be able to create a profile based on a yaml file on disk
	tests := []struct {
		description string
		contents    string
		expected    string
	}{
		{"normal", "prompt: new-profile", "prompt: new-profile"},

		// `rpk print profile -v` outputs file _and_ effective config (w/ overrides)
		// if we see that, make sure we're only using the file-based output
		{"verbose", profilePrintVerboseOutput, "prompt: file-based"},
	}

	for _, tt := range tests {
		t.Run(tt.description, func(t *testing.T) {
			fs := afero.NewMemMapFs()

			// Write the given profile to the fs instance and then use it to create a profile
			require.NoError(t, afero.WriteFile(fs, "/new.yaml", []byte(tt.contents), 0o644), "error writing test profile")

			err := CreateFlow(
				t.Context(),
				fs,
				&config.Config{},
				&config.RpkYaml{},
				&config.RpkCloudAuth{},
				"",
				"/new.yaml",
				"",
				false,
				[]string{},
				"new",
				"new profile",
				"",
			)

			require.NoError(t, err, "error in CreateFlow")

			// Load from the default config location and make sure it contains the new profile
			configDir, err := os.UserConfigDir()
			require.NoError(t, err, "error getting os.UserConfigDir")

			configPath := filepath.Join(configDir, "rpk", "rpk.yaml")

			buf, err := afero.ReadFile(fs, configPath)
			require.NoError(t, err, "error reading default rpk.yaml")

			contents := string(buf)
			require.Contains(t, contents, tt.expected)

			// profilePrintVerboseOutput contains two profiles, make sure we didn't write the effective config
			require.NotContains(t, contents, "effective-config")
		})
	}
}
