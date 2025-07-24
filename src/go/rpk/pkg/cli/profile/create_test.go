package profile

import (
	"testing"

	controlplanev1 "buf.build/gen/go/redpandadata/cloud/protocolbuffers/go/redpanda/api/controlplane/v1"
	"github.com/stretchr/testify/require"
)

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
