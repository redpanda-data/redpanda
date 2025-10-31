package profile

import (
	"os"
	"path/filepath"
	"testing"

	controlplanev1beta2 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1beta2"
	"github.com/redpanda-data/redpanda/src/go/rpk/pkg/config"
	"github.com/spf13/afero"
	"github.com/stretchr/testify/require"
)

func TestCombineClusterNames(t *testing.T) {
	tests := []struct {
		name string
		rgs  []*controlplanev1beta2.ResourceGroup
		scs  []*controlplanev1beta2.ServerlessCluster
		cs   []*controlplanev1beta2.Cluster
		exp  namesAndClusters
	}{
		{
			name: "combine Serverless Clusters and Clusters",
			rgs: []*controlplanev1beta2.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
				{Id: "rg2", Name: "ResourceGroup2"},
			},
			scs: []*controlplanev1beta2.ServerlessCluster{
				{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1beta2.ServerlessCluster_STATE_READY},
				{ResourceGroupId: "rg2", Name: "SC2", State: controlplanev1beta2.ServerlessCluster_STATE_READY},
				{ResourceGroupId: "rg1", Name: "SC3", State: controlplanev1beta2.ServerlessCluster_STATE_CREATING}, // should not appear if it's not ready.
			},
			cs: []*controlplanev1beta2.Cluster{
				{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1beta2.Cluster_STATE_READY},
				{ResourceGroupId: "rg2", Name: "C2", State: controlplanev1beta2.Cluster_STATE_DELETING}, // should not appear if it's not ready.
				{ResourceGroupId: "rg2", Name: "C3", State: controlplanev1beta2.Cluster_STATE_READY},
			},
			exp: namesAndClusters{
				{name: "ResourceGroup1/SC1", sc: &controlplanev1beta2.ServerlessCluster{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1beta2.ServerlessCluster_STATE_READY}},
				{name: "ResourceGroup2/SC2", sc: &controlplanev1beta2.ServerlessCluster{ResourceGroupId: "rg2", Name: "SC2", State: controlplanev1beta2.ServerlessCluster_STATE_READY}},
				{name: "ResourceGroup1/C1", c: &controlplanev1beta2.Cluster{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1beta2.Cluster_STATE_READY}},
				{name: "ResourceGroup2/C3", c: &controlplanev1beta2.Cluster{ResourceGroupId: "rg2", Name: "C3", State: controlplanev1beta2.Cluster_STATE_READY}},
			},
		},
		{
			name: "empty inputs",
			rgs:  []*controlplanev1beta2.ResourceGroup{},
			scs:  []*controlplanev1beta2.ServerlessCluster{},
			cs:   []*controlplanev1beta2.Cluster{},
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
			rgs: []*controlplanev1beta2.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
			},
			scs: []*controlplanev1beta2.ServerlessCluster{
				{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1beta2.ServerlessCluster_STATE_READY},
			},
			cs: []*controlplanev1beta2.Cluster{},
			exp: namesAndClusters{
				{name: "ResourceGroup1/SC1", sc: &controlplanev1beta2.ServerlessCluster{ResourceGroupId: "rg1", Name: "SC1", State: controlplanev1beta2.ServerlessCluster_STATE_READY}},
			},
		},
		{
			name: "Clusters only",
			rgs: []*controlplanev1beta2.ResourceGroup{
				{Id: "rg1", Name: "ResourceGroup1"},
			},
			scs: []*controlplanev1beta2.ServerlessCluster{},
			cs: []*controlplanev1beta2.Cluster{
				{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1beta2.Cluster_STATE_READY},
			},
			exp: namesAndClusters{
				{name: "ResourceGroup1/C1", c: &controlplanev1beta2.Cluster{ResourceGroupId: "rg1", Name: "C1", State: controlplanev1beta2.Cluster_STATE_READY}},
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
		// {"verbose", profilePrintVerboseOutput, "prompt: file-based"},
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
